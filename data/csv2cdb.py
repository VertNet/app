#!/usr/bin/env python

# Copyright 2012 Aaron Steele and University of California at Berkeley
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

__author__ = "Aaron Steele"

"""This module supports uploading a CSV file to CartoDB over the SQL API.

1) download taxon table into memory: {name:cartodb_id}
2) for all names in CSV not in taxon table, bulk insert to taxon table

"""

from cartodb import CartoDB, CartoDBException

import collections
import csv_unicode as csvu
import csv
import json
import logging
import os
import sys
import time
import threading
import zipfile

from Queue import Queue
from optparse import OptionParser
from ssl import SSLError
from httplib import BadStatusLine


TAXON_CONCEPTS = ['kingdom', 'phylum', 'class', 'order', 'family', 'genus', 
                  'species', 'scientificname']

global TAXON_TABLE 
TAXON_TABLE = {}

class Query(object):

    def __init__(self, queue, cdb, query):
        self.queue = queue
        self.cdb = cdb
        self.query = query

    def execute(self, params):
        retry_count = 10
        backoff = 1
        errors = []
        response = None
        query = self.prepare_query(params)

        while retry_count > 0:
            try:
                response = self.cdb.sql(query)
            except CartoDBException as e:
                self.handle(query, params, None, e)
            except Exception as e:
                logging.info("Retry %s with backoff %s for %s - %s" % (retry_count, backoff, params, e))
                errors.append(e)
                time.sleep(backoff)
                if backoff < 8:
                    backoff *= 2
                retry_count -= 1
        
        self.handle(query, params, response, errors)

    def loop(self):
        while True:
            r = self.queue.get()
            if r == None:
                self.queue.task_done()
                break
            else:
                (params) = r
            self.execute(params)
            self.queue.task_done()

class TaxonQuery(Query):
    
    def prepare_query(self, params):
        query = ''
        for name in params['names']:
            query += self.query % dict(name=name) + ';'
        return query
        
    def handle(self, query, params, response, errors):
        logging.info('%s %s' % (response, errors))
        
def get_options():
    """Parses and returns command line options."""

    parser = OptionParser()

    parser.add_option("-c", "--csv_file", dest="csv_file",
                      help="The CSV file to upload",
                      default=None)
    parser.add_option("-k", "--consumer_key", dest="consumer_key",
                      help="The CartoDB consumer key",
                      default=None)
    parser.add_option("-s", "--consumer_secret", dest="consumer_secret",
                      help="The CartoDB consumer secret",
                      default=None)
    parser.add_option("-u", "--user", dest="user",
                      help="The CartoDB user")
    parser.add_option("-p", "--password", dest="password",
                      help="The CartoDB user password")
    parser.add_option("-d", "--domain", dest="domain",
                      help="The CartoDB domain")

    (options, args) = parser.parse_args()

    return options

def taxons_from_csv(path):
    """Returns a multimap of taxon concept to set of names."""
    taxons = collections.defaultdict(set)
    for row in csvu.UnicodeDictReader(open(path, 'r')):
        row = dict((k.lower(), v) for k, v in row.iteritems()) # lowercase keys
        for taxon in TAXON_CONCEPTS:
            name = row.get(taxon)
            if name:
                taxons[taxon].add(name.strip().lower())
    return taxons

def get_taxon_table(cdb):
    taxon_table = {}
    response = cdb.sql("SELECT name, cartodb_id FROM taxon")
    for row in response['rows']:
        taxon_table[row['name'].lower()] = row['cartodb_id']
    return taxon_table

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)    

    options = get_options()
    
    cdb = CartoDB(
        options.consumer_key, 
        options.consumer_secret, 
        options.user, 
        options.password, 
        options.domain)
    
    queue = Queue()
    renderers = {}
    num_threads = 40

    query = "INSERT INTO taxon (name) VALUES ('%(name)s')"

    for i in range(num_threads): # number of threads
        renderer = TaxonQuery(queue, cdb, query)
        render_thread = threading.Thread(target=renderer.loop)
        render_thread.start()
        renderers[i] = render_thread

    taxons = taxons_from_csv(options.csv_file)
    taxon_table = get_taxon_table(cdb)

    uniques = set()
    for names in taxons.values():
        uniques.update(names)

    batch_size = 500
    names = []
    for name in uniques:
        if not taxon_table.has_key(name):
            if len(names) < batch_size:
                names.append(name)
            else:
                queue.put(dict(names=names))
                names = []
    if len(names) > 0:
        queue.put(dict(names=names))
    
    for i in range(num_threads):
        queue.put(None)

    # wait for pending rendering jobs to complete
    queue.join()
    for i in range(num_threads):
        renderers[i].join()

    # Reload taxon table into memory:
    taxon_table = get_taxon_table(cdb)

    taxon_locations = {} # name:{column: cartodb_id}
    for taxon, names in taxons.iteritems():
        for name in names:
            taxon_locations[name] = {'taxon_%s_cartodb_id' % taxon: taxon_table[name]}
    logging.info(taxon_locations)

    logging.info(TAXON_TABLE)
    logging.info('CSV successfully uploaded to CartoDB .')



"""
1) load CSV and insert names into taxon table
2) load taxon table into memory
3) insert into taxon

"""

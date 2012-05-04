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

"""This module supports uploading a CSV file to CartoDB over the SQL API."""

from cartodb import CartoDB, CartoDBException

import csv_unicode as csvu
import csv
import json
import logging
import os
import sys
import threading
import zipfile

from Queue import Queue
from optparse import OptionParser

TAXON_CONCEPTS = ['kingdom', 'phylum', 'class', 'order', 'family', 'genus', 
                  'species', 'scientificname']

global TAXON_TABLE 
TAXON_TABLE = {}

class Query(object):

    def __init__(self, q, cdb):
        self.q = q
        self.cdb = cdb

    def execute(self, data):
        global TAXON_TABLE
        name = data['name']
        taxon = data['taxon']

        if TAXON_TABLE.has_key(name):
            return

        try:
            query = "SELECT cartodb_id FROM taxon WHERE lower(name) = '%s'" % name.lower()
            response = self.cdb.sql(query)
            rows = response['rows']
            
            if len(rows) == 0:
                query = "INSERT INTO taxon (name) VALUES ('%s') RETURNING cartodb_id" % name
                response = self.cdb.sql(query)
                rows = response['rows']
                logging.info("Inserted %s with response %s" % (name, response))
            else:
                logging.info("Selected %s with response %s" % (name, response))

            cartodb_id = rows[0]['cartodb_id']
            TAXON_TABLE[name] = cartodb_id

        except CartoDBException as e:
            logging.error('Query failed because %s (%s)' % (query, e))
        except:
            logging.error('Query failed: %s' % query)

    def loop(self):
        while True:
            # Fetch a query from the queue and run the query
            r = self.q.get()
            if r == None:
                self.q.task_done()
                break
            else:
                (data) = r
            self.execute(data)
            self.q.task_done()

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

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)    

    options = get_options()
    
    cdb = CartoDB(
        options.consumer_key, 
        options.consumer_secret, 
        options.user, 
        options.password, 
        options.domain)
    
    count = 10    
    queue = Queue()
    renderers = {}
    num_threads = 50

    for i in range(num_threads): # number of threads
        renderer = Query(queue, cdb)
        render_thread = threading.Thread(target=renderer.loop)
        render_thread.start()
        renderers[i] = render_thread

    # Fill the queue with taxon names:
    for row in csvu.UnicodeDictReader(open(options.csv_file, 'r')):
        row = dict((k.lower(), v) for k, v in row.iteritems()) # lowercase keys
        for taxon in TAXON_CONCEPTS:
            if not row.has_key(taxon):
                continue
            name = row[taxon]
            if not name:
                continue
            queue.put(dict(taxon=taxon, name=name))

    for i in range(num_threads):
        queue.put(None)

    # wait for pending rendering jobs to complete
    queue.join()
    for i in range(num_threads):
        renderers[i].join()

    logging.info(TAXON_TABLE)
    logging.info('CSV successfully uploaded to CartoDB .')

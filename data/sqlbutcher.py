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

terms = ['acceptednameusage', 'acceptednameusageid', 'accessrights', 'associatedmedia', 'associatedoccurrences', 'associatedreferences', 'associatedsequences', 'associatedtaxa', 'basisofrecord', 'bed', 'behavior', 'bibliographiccitation', 'catalognumber', 'class', 'collectioncode', 'collectionid', 'continent', 'coordinateprecision', 'coordinateuncertaintyinmeters', 'country', 'countrycode', 'county', 'datageneralizations', 'datasetid', 'datasetname', 'dateidentified', 'day', 'decimallatitude', 'decimallongitude', 'disposition', 'dynamicproperties', 'earliestageorloweststage', 'earliesteonorlowesteonothem', 'earliestepochorlowestseries', 'earliesteraorlowesterathem', 'earliestperiodorlowestsystem', 'enddayofyear', 'establishmentmeans', 'eventdate', 'eventid', 'eventremarks', 'eventtime', 'family', 'fieldnotes', 'fieldnumber', 'footprintspatialfit', 'footprintsrs', 'footprintwkt', 'formation', 'genus', 'geodeticdatum', 'geologicalcontextid', 'georeferencedby', 'georeferenceddate', 'georeferenceprotocol', 'georeferenceremarks', 'georeferencesources', 'georeferenceverificationstatus', 'group', 'habitat', 'higherclassification', 'highergeography', 'highergeographyid', 'highestbiostratigraphiczone', 'identificationid', 'identificationqualifier', 'identificationreferences', 'identificationremarks', 'identificationverificationstatus', 'identifiedby', 'individualcount', 'individualid', 'informationwithheld', 'infraspecificepithet', 'institutioncode', 'institutionid', 'island', 'islandgroup', 'kingdom', 'language', 'latestageorhigheststage', 'latesteonorhighesteonothem', 'latestepochorhighestseries', 'latesteraorhighesterathem', 'latestperiodorhighestsystem', 'lifestage', 'lithostratigraphicterms', 'locality', 'locationaccordingto', 'locationid', 'locationremarks', 'lowestbiostratigraphiczone', 'maximumdepthinmeters', 'maximumdistanceabovesurfaceinmeters', 'maximumelevationinmeters', 'measurementaccuracy', 'measurementdeterminedby', 'measurementdetermineddate', 'measurementid', 'measurementmethod', 'measurementremarks', 'measurementtype', 'measurementunit', 'measurementvalue', 'member', 'minimumdepthinmeters', 'minimumdistanceabovesurfaceinmeters', 'minimumelevationinmeters', 'modified', 'month', 'municipality', 'nameaccordingto', 'nameaccordingtoid', 'namepublishedin', 'namepublishedinid', 'namepublishedinyear', 'nomenclaturalcode', 'nomenclaturalstatus', 'occurrenceid', 'occurrenceremarks', 'occurrencestatus', 'order', 'originalnameusage', 'originalnameusageid', 'othercatalognumbers', 'ownerinstitutioncode', 'parentnameusage', 'parentnameusageid', 'phylum', 'pointradiusspatialfit', 'preparations', 'previousidentifications', 'recordedby', 'recordnumber', 'references', 'relatedresourceid', 'relationshipaccordingto', 'relationshipestablisheddate', 'relationshipofresource', 'relationshipremarks', 'reproductivecondition', 'resourceid', 'resourcerelationshipid', 'rights', 'rightsholder', 'samplingeffort', 'samplingprotocol', 'scientificname', 'scientificnameauthorship', 'scientificnameid', 'sex', 'specificepithet', 'startdayofyear', 'stateprovince', 'subgenus', 'taxonconceptid', 'taxonid', 'taxonomicstatus', 'taxonrank', 'taxonremarks', 'type', 'typestatus', 'verbatimcoordinates', 'verbatimcoordinatesystem', 'verbatimdepth', 'verbatimelevation', 'verbatimeventdate', 'verbatimlatitude', 'verbatimlocality', 'verbatimlongitude', 'verbatimsrs', 'verbatimtaxonrank', 'vernacularname', 'waterbody', 'year'] 

col_types = {'datasetname': 'text', 'occurrenceremarks': 'text', 'namepublishedin': 'text', 'geologicalcontextid': 'text', 'associatedreferences': 'text', 'month': 'int4', 'decimallongitude': 'text', 'fieldnotes': 'text', 'verbatimlongitude': 'text', 'highergeography': 'text', 'modified': 'text', 'startdayofyear': 'int4', 'minimumelevationinmeters': 'numeric', 'resourcerelationshipid': 'text', 'continent': 'text', 'measurementmethod': 'text', 'relationshipremarks': 'text', 'measurementtype': 'text', 'group': 'text', 'accessrights': 'text', 'locationid': 'text', 'measurementdeterminedby': 'text', 'maximumdistanceabovesurfaceinmeters': 'numeric', 'kingdom': 'text', 'identificationverificationstatus': 'text', 'cartodb_id': 'int4', 'coordinateprecision': 'numeric', 'verbatimcoordinatesystem': 'text', 'verbatimsrs': 'text', 'parentnameusageid': 'text', 'latesteraorhighesterathem': 'text', 'day': 'int4', 'identificationid': 'text', 'occurrenceid': 'text', 'earliestageorloweststage': 'text', 'earliesteonorlowesteonothem': 'text', 'measurementunit': 'text', 'footprintsrs': 'text', 'samplingeffort': 'text', 'identificationqualifier': 'text', 'names_cartodb_id': 'float8', 'phylum': 'text', 'originalnameusageid': 'text', 'datageneralizations': 'text', 'coordinateuncertaintyinmeters': 'numeric', 'higherclassification': 'text', 'habitat': 'text', 'lifestage': 'text', 'namepublishedinid': 'text', 'collectioncode': 'text', 'latestageorhigheststage': 'text', 'earliestperiodorlowestsystem': 'text', 'verbatimlatitude': 'text', 'year': 'int4', 'specificepithet': 'text', 'verbatimtaxonrank': 'text', 'relationshipestablisheddate': 'text', 'basisofrecord': 'text', 'geodeticdatum': 'text', 'latesteonorhighesteonothem': 'text', 'acceptednameusage': 'text', 'measurementvalue': 'text', 'parentnameusage': 'text', 'verbatimeventdate': 'text', 'order': 'text', 'recordedby': 'text', 'earliesteraorlowesterathem': 'text', 'samplingprotocol': 'text', 'taxonid': 'text', 'formation': 'text', 'disposition': 'text', 'measurementremarks': 'text', 'the_geom_webmercator': 'geometry', 'language': 'text', 'institutionid': 'text', 'island': 'text', 'occurrencestatus': 'text', 'ownerinstitutioncode': 'text', 'nomenclaturalstatus': 'text', 'genus': 'text', 'datasetid': 'text', 'georeferenceprotocol': 'text', 'eventremarks': 'text', 'family': 'text', 'scientificnameid': 'text', 'measurementaccuracy': 'text', 'stateprovince': 'text', 'municipality': 'text', 'nameaccordingtoid': 'text', 'county': 'text', 'georeferenceddate': 'text', 'references': 'text', 'associatedoccurrences': 'text', 'georeferencedby': 'text', 'earliestepochorlowestseries': 'text', 'taxonrank': 'text', 'verbatimlocality': 'text', 'measurementid': 'text', 'identificationreferences': 'text', 'countrycode': 'text', 'institutioncode': 'text', 'highergeographyid': 'text', 'relationshipaccordingto': 'text', 'latestperiodorhighestsystem': 'text', 'maximumelevationinmeters': 'numeric', 'nameaccordingto': 'text', 'typestatus': 'text', 'type': 'text', 'taxonconceptid': 'text', 'eventid': 'text', 'eventtime': 'text', 'islandgroup': 'text', 'verbatimdepth': 'text', 'preparations': 'text', 'measurementdetermineddate': 'text', 'pointradiusspatialfit': 'text', 'georeferenceremarks': 'text', 'footprintspatialfit': 'text', 'rights': 'text', 'dynamicproperties': 'text', 'georeferenceverificationstatus': 'text', 'sex': 'text', 'infraspecificepithet': 'text', 'bed': 'text', 'fieldnumber': 'text', 'behavior': 'text', 'country': 'text', 'taxonomicstatus': 'text', 'taxonremarks': 'text', 'eventdate': 'text', 'relatedresourceid': 'text', 'namepublishedinyear': 'text', 'individualcount': 'text', 'verbatimelevation': 'text', 'rightsholder': 'text', 'subgenus': 'text', 'bibliographiccitation': 'text', 'verbatimcoordinates': 'text', 'updated_at': 'timestamp', 'locations_cartodb_id': 'float8', 'georeferencesources': 'text', 'nomenclaturalcode': 'text', 'waterbody': 'text', 'dateidentified': 'text', 'catalognumber': 'text', 'id': 'text', 'originalnameusage': 'text', 'locality': 'text', 'relationshipofresource': 'text', 'resourceid': 'text', 'member': 'text', 'locationremarks': 'text', 'minimumdistanceabovesurfaceinmeters': 'numeric', 'informationwithheld': 'text', 'scientificnameauthorship': 'text', 'recordnumber': 'text', 'lowestbiostratigraphiczone': 'text', 'collectionid': 'text', 'acceptednameusageid': 'text', 'individualid': 'text', 'footprintwkt': 'text', 'maximumdepthinmeters': 'numeric', 'scientificname': 'text', 'highestbiostratigraphiczone': 'text', 'the_geom': 'geometry', 'class': 'text', 'vernacularname': 'text', 'previousidentifications': 'text', 'identificationremarks': 'text', 'decimallatitude': 'numeric', 'minimumdepthinmeters': 'numeric', 'latestepochorhighestseries': 'text', 'created_at': 'timestamp', 'locationaccordingto': 'text', 'othercatalognumbers': 'text', 'establishmentmeans': 'text', 'identifiedby': 'text', 'associatedmedia': 'text', 'associatedsequences': 'text', 'associatedtaxa': 'text', 'lithostratigraphicterms': 'text', 'reproductivecondition': 'text', 'original': 'text', 'enddayofyear': 'int4'}


nysm_terms = [term.lower() for term in ["id","fieldNumber","county","minimumElevationInMeters","coordinateUncertaintyInMeters","georeferenceProtocol","lifeStage","geodeticDatum","country","occurrenceRemarks","family","higherClassification","sex","catalogNumber","institutionCode","continent","stateProvince","decimalLatitude","modified","day","islandGroup","order","individualCount","infraspecificEpithet","georeferenceRemarks","locality","specificEpithet","class","scientificName","recordNumber","verbatimEventDate","year","phylum","preparations","decimalLongitude","collectionCode","island","verbatimElevation","higherGeography","recordedBy","kingdom","eventTime","genus","verbatimCoordinateSystem","basisOfRecord","maximumElevationInMeters","month"]]

# (term, term, term_type)
case = "CASE WHEN %s='' THEN null ELSE %s::%s END"

"""
1) get terms from csv
2) build select statement
3) post

"""

def get_columns(csv_file):
    cols = [column.strip().lower().replace('"', '') 
            for column in open(csv_file, 'r').readline().split(',')]
    cols.sort()
    return cols

def get_options():
    """Parses and returns command line options."""

    parser = OptionParser()

    parser.add_option("-c", "--csv_file", dest="csv_file",
                      help="The CSV file to upload",
                      default=None)
    parser.add_option("-t", "--table_name", dest="table_name",
                      help="The CartoDB table name.",
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
    
    cols = get_columns(options.csv_file)

    sql = 'INSERT INTO occurrence (%s) (%s);'

    cases = []
    for col in cols:
        col_type = col_types[col]
        col_case = case % (col, col, col_type)
        cases.append(col_case)


    select = 'SELECT %s FROM %s' % (reduce(lambda x,y: '%s,%s' % (x, y), cases), options.table_name)

    # Prints out an SQL statement that inserts into occurrence table from another.
    print sql % (reduce(lambda x,y: '%s,%s' % (x, y), cols), select)

    

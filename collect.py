#!/usr/bin/env python3

# Constants
JSON_URL = 'https://services.arcgis.com/ORpvigFPJUhb8RDF/arcgis/rest/services/corona_DD_7_Sicht/FeatureServer/0/query?f=json&where=ObjectId>=0&outFields=*'
CACHED_JSON_FILENAME = 'cached.json'

# Debugging
from IPython import embed

# Command line arguments
import argparse

# Logging
import logging

# Paths
import sys
import pathlib

# JSON
import urllib.request
import json

# Date parsing
import dateutil.parser
from datetime import datetime

# Database
from influxdb import InfluxDBClient

def setup():
    """Performs some basic configuration regarding the database.
    """
    # setup logging
    logging_format = '%(asctime)s %(levelname)s %(message)s' # %(name)s.%(funcName)s %(pathname)s:
    logging.basicConfig(level=logging.INFO, format=logging_format)
    logger = logging.getLogger()

    # Read command line arguments.
    argparser = argparse.ArgumentParser(description='Collect official infection statistics published by the city of Dresden.')
    argparser.add_argument('-f', '--file', help='load JSON data from a local file instead from server', nargs='?', type=argparse.FileType('r'), const='query.json') # default=sys.stdin; https://stackoverflow.com/a/15301183/7192373
    argparser.add_argument('-v', '--verbose', help='print debug messages', action='store_true')

    global args
    args = argparser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
        logger.info('Debug output turned on.')

    # get absolute path of this Python file
    global abs_python_file_path
    abs_python_file_path = pathlib.Path(__file__).resolve()

    # setup DB connection
    global db_client
    db_client = InfluxDBClient(host='localhost', port=8086) # https://www.influxdata.com/blog/getting-started-python-influxdb/
    db_client.create_database('corona_dd')
    db_client.switch_database('corona_dd')

def main():
    setup()
    logger = logging.getLogger()

    # load locally cached JSON file
    json_file_path = pathlib.Path(abs_python_file_path.parent, CACHED_JSON_FILENAME)
    try:
        with open(json_file_path, 'r') as json_file:
            cached_data = json.load(json_file)
    except FileNotFoundError:
        cached_data = None
        logger.debug('File \'{:s}\' not found.'.format(CACHED_JSON_FILENAME))

    # load (possibly) new JSON data and write it to InfluxDB
    if args.file:
        data = json.load(args.file)
        logger.debug('Read JSON data from local file {:s}.'.format(args.file.name))
    else:
        with urllib.request.urlopen(JSON_URL) as response:
            data = json.load(response)
            logger.debug('Downloaded JSON data from server.')
        
    # check whether downloaded JSON contains new data
    if data == cached_data:
        logger.debug('JSON data has not changed.')
    else:
        logger.debug('New JSON data found!')

        # cache JSON file
        with open(json_file_path, 'w') as json_file:
            json.dump(data, json_file)

        # generate time series list according to the expected InfluxDB line protocol: https://docs.influxdata.com/influxdb/v1.8/write_protocols/line_protocol_tutorial/
        data_timestamp = datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
        time_series = []
        for measurement in data['features']:
            point = {
                'measurement'   : 'dresden_official',
                'tags'          : { # metadata for the data point
                    'pub_date'  : data_timestamp, # date on which the record was published
                    },
                'time'          : datetime.isoformat(dateutil.parser.parse(measurement['attributes'].pop('Datum'), dayfirst=True)), # parse date, switch month and day and generate ISO 8601 formatted string
                'fields'        : { # in principle, a simple "measurement.pop('attributes')" also works, but unfortunately the field datatype is defined by the first point (in case of this foreign dataset, some fields arefilled with NoneType) written to a series; https://github.com/influxdata/influxdb/issues/3460#issuecomment-124747104
                    'Anzeige_Indikator'             : str(measurement['attributes']['Anzeige_Indikator']), # value is either None or 'x'
                    'BelegteBetten'                 : int(measurement['attributes']['BelegteBetten'] or 0), # replace NoneType with 0
                    'Datum_neu'                     : int(measurement['attributes']['Datum_neu'] or 0),
                    'Fallzahl'                      : int(measurement['attributes']['Fallzahl'] or 0),
                    'Genesungsfall'                 : int(measurement['attributes']['Genesungsfall'] or 0),
                    'Hospitalisierung'              : int(measurement['attributes']['Hospitalisierung'] or 0),
                    'Inzidenz'                      : float(measurement['attributes']['Inzidenz'] or 0),
                    'ObjectId'                      : int(measurement['attributes']['ObjectId'] or 0),
                    'Sterbefall'                    : int(measurement['attributes']['Sterbefall'] or 0),
                    'Zuwachs_Fallzahl'              : int(measurement['attributes']['Zuwachs_Fallzahl'] or 0),
                    'Zuwachs_Genesung'              : int(measurement['attributes']['Zuwachs_Genesung'] or 0),
                    'Zuwachs_Krankenhauseinweisung' : int(measurement['attributes']['Zuwachs_Krankenhauseinweisung'] or 0),
                    'Zuwachs_Sterbefall'            : int(measurement['attributes']['Zuwachs_Sterbefall'] or 0),
                }
            }
            time_series.append(point)

        # write data to database
        db_client.write_points(time_series)

if __name__ == '__main__':
    main()
#!/usr/bin/env python3

# constants
RELEASE = 'v0.2.0'
JSON_URL = 'https://services.arcgis.com/ORpvigFPJUhb8RDF/arcgis/rest/services/corona_DD_7_Sicht/FeatureServer/0/query?f=json&where=ObjectId>=0&outFields=*'
CACHED_JSON_FILENAME = 'cached.json'

# debugging
from IPython import embed

# command line arguments
import argparse

# logging
import logging
import logging.handlers

# paths
import pathlib

# JSON
import urllib.request
import json

# date parsing
import dateutil.parser
from datetime import datetime, timezone

# database
from influxdb import InfluxDBClient

def setup():
    """Performs some basic configuration regarding logging, command line options, database etc.
    """
    # get absolute path of this Python file
    global abs_python_file_dir
    abs_python_file_dir = pathlib.Path(__file__).resolve().parent
    log_filename = '{}{:s}'.format(pathlib.Path(__file__).resolve().stem, '.log')

    # read command line arguments (https://docs.python.org/3/howto/argparse.html)
    argparser = argparse.ArgumentParser(description='Collect official SARS-CoV-2 infection statistics published by the city of Dresden.')
    argparser.add_argument('-a', '--archive-json', help='archive JSON file each time new data is found or force-collected', action='store_true')
    argparser.add_argument('-c', '--force-collect', help='store JSON data, regardless of whether new data points have been found or not', action='store_true')
    argparser.add_argument('-d', '--date', help='set publishing date manually for the new data set, e. g. \'2020-10-18T09:52:41Z\', otherwise current time (UTC) is used')
    argparser.add_argument('-f', '--file', help='load JSON data from a local file instead from server', nargs='?', type=argparse.FileType('r'), const='query.json') # default=sys.stdin; https://stackoverflow.com/a/15301183/7192373
    argparser.add_argument('-l', '--log', help='save log in file \'{:s}\''.format(log_filename), action='store_true')
    argparser.add_argument('-v', '--verbose', help='print debug messages', action='store_true')

    global args
    args = argparser.parse_args()

    if args.verbose:
        log_level = logging.DEBUG
    else:
        log_level = logging.INFO

    # setup logging
    global logger
    logger = logging.getLogger()
    logger.setLevel(log_level)

    # log format
    logging_format = '%(asctime)s %(levelname)s %(message)s' # %(name)s.%(funcName)s %(pathname)s:
    log_formatter = logging.Formatter(logging_format) #, datefmt="%Y-%m-%dT%H:%M:%S")

    # log to console
    handler = logging.StreamHandler()
    handler.setFormatter(log_formatter)
    logger.addHandler(handler)

    # log to file
    if args.log:
        handler = logging.handlers.RotatingFileHandler(pathlib.Path(abs_python_file_dir, log_filename), maxBytes=2**20, backupCount=5) # https://stackoverflow.com/a/13733777/7192373; https://docs.python.org/3/library/logging.handlers.html#logging.handlers.RotatingFileHandler
        handler.setFormatter(log_formatter)
        logger.addHandler(handler)

    # setup DB connection
    global db_client
    db_client = InfluxDBClient(host='localhost', port=8086) # https://www.influxdata.com/blog/getting-started-python-influxdb/
    db_client.create_database('corona_dd')
    db_client.switch_database('corona_dd')

def main():
    setup()

    # load locally cached JSON file
    json_file_path = pathlib.Path(abs_python_file_dir, CACHED_JSON_FILENAME)
    try:
        with open(json_file_path, 'r') as json_file:
            cached_data = json.load(json_file)
    except FileNotFoundError:
        cached_data = None
        logger.debug('File \'{:s}\' not found.'.format(CACHED_JSON_FILENAME))

    # load (possibly) new JSON data and write it to InfluxDB
    if args.file:
        data = json.load(args.file)
        logger.debug('Read JSON data from local file \'{:s}\'.'.format(args.file.name))
    else:
        with urllib.request.urlopen(JSON_URL) as response:
            data = json.load(response)
            logger.debug('Downloaded JSON data from server.')
        
    # check whether downloaded JSON contains new data or user enforced data collection
    if data == cached_data and not args.force_collect:
        logger.info('Data has not changed.')
    else:
        if data != cached_data:
            logger.info('Found new data!')
        else:
            logger.info('Data has not changed, but is nevertheless collected as requested.')

        # save query date
        if args.date:
            data_pub_date = dateutil.parser.parse(args.date) # use user's publishing date if given for the new data set
        else:
            data_pub_date = datetime.now(tz=timezone.utc) # use current time

        # cache JSON file
        with open(json_file_path, 'w') as json_file:
            json.dump(data, json_file)
        # archive JSON file
        if args.archive_json:
            archive_file_dir = pathlib.Path(abs_python_file_dir, 'json-archive')
            pathlib.Path.mkdir(archive_file_dir, exist_ok=True)
            archive_file_path = pathlib.Path(archive_file_dir, '{:s}.json'.format(data_pub_date.strftime('%Y-%m-%dT%H.%M.%SZ')))
            with open(archive_file_path, 'w') as json_file:
                json.dump(data, json_file)

        # generate time series list according to the expected InfluxDB line protocol: https://docs.influxdata.com/influxdb/v1.8/write_protocols/line_protocol_tutorial/
        time_series = []
        for measurement in data['features']:
            point = {
                'measurement'   : 'dresden_official',
                'tags'          : { # metadata for the data point
                    'script_version'    : RELEASE, # state version numer of this script
                    'pub_date'          : data_pub_date.strftime('%Y-%m-%dT%H:%M:%S'), # date on which the record was published
                    },
                'time'          : int(dateutil.parser.parse(measurement['attributes'].pop('Datum'), dayfirst=True).replace(tzinfo=timezone.utc).timestamp()), # parse date, switch month and day, explicetely set UTC (InfluxDB uses UTC), otherwise local timezone is assumed; 'datetime.isoformat()': generate ISO 8601 formatted string (e. g. '2020-10-22T21:30:13.883657+00:00')
                'fields'        : { # in principle, a simple "measurement.pop('attributes')" also works, but unfortunately the field datatype is defined by the first point written to a series (in case of this foreign data set, some fields are filled with NoneType); https://github.com/influxdata/influxdb/issues/3460#issuecomment-124747104
                    # own fields
                    'pub_date_seconds'              : int(data_pub_date.timestamp()), # add better searchable UNIX timestamp in seconds in addition to the human readable 'pub_date' tag; https://docs.influxdata.com/influxdb/v2.0/reference/glossary/#unix-timestamp; POSIX timestamps in Python: https://stackoverflow.com/a/8778548/7192373
                    # fields from data source
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
        db_client.write_points(time_series, time_precision='s')
        logger.info('Time series successfully written to database.')

if __name__ == '__main__':
    main()
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# constants
RELEASE = 'v0.3.0'
JSON_URL = 'https://services.arcgis.com/ORpvigFPJUhb8RDF/arcgis/rest/services/corona_DD_7_Sicht/FeatureServer/0/query?f=pjson&where=ObjectId>=0&outFields=*'
CACHED_JSON_FILENAME = 'cached.json'
JSON_ARCHIVE_FOLDER = 'json-archive'

INFLUXDB_DATABASE = 'corona_dd'
INFLUXDB_MEASUREMENTS = ['dresden_official', 'dresden_official_all'] # all measurements to be saved
INFLUXDB_MEASUREMENT_ARCHIVE = 'dresden_official_all' # measurement that contains all series (latest of the day as well as all corrections by the city)

import sys

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
import datetime

# dicts
import copy

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
    argparser = argparse.ArgumentParser(description='Collects official SARS-CoV-2 infection statistics published by the city of Dresden.')
    arggroup = argparser.add_mutually_exclusive_group()
    argparser.add_argument('-a', '--archive-json', help='archive JSON file each time new data is found or force-collected', action='store_true')
    argparser.add_argument('-c', '--force-collect', help='store JSON data, regardless of whether new data points have been found or not', action='store_true')
    arggroup.add_argument('-d', '--date', help='set publishing date manually for the new data set, e. g. \'2020-10-18T09:52:41Z\'')
    argparser.add_argument('-f', '--file', help='load JSON data from a local file instead from server; if no publishing date is passed with the \'--date\' option, an attempt is made to read the date from the filename', nargs='?', type=argparse.FileType('r'), const='query.json') # default=sys.stdin; https://stackoverflow.com/a/15301183/7192373
    argparser.add_argument('-l', '--log', help='save log in file \'{:s}\''.format(log_filename), action='store_true')
    argparser.add_argument('-n', '--no-cache', help='suppress the saving of a JSON cache file (helpful if you do not want to mess with an active cron job looking for changes)', action='store_true')
    argparser.add_argument('-s', '--skip-influxdb', help='check for and write new JSON data only, do not write to InfluxDB', action='store_true')
    arggroup.add_argument('-t', '--auto-date', help='do not try to to parse the publishing date from the filename, instead write current date (UTC) to database', action='store_true')
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
    if not args.skip_influxdb:
        global db_client
        db_client = InfluxDBClient(host='localhost', port=8086) # https://www.influxdata.com/blog/getting-started-python-influxdb/
        db_client.create_database(INFLUXDB_DATABASE)
        db_client.switch_database(INFLUXDB_DATABASE)

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
   
    # get current date from system and latest entry date from the data set
    data_load_date = datetime.datetime.now(tz=datetime.timezone.utc)
    midnight = data_load_date.replace(hour = 0, minute = 0, second = 0, microsecond = 0)
    data_latest_date = datetime.datetime.fromtimestamp(data['features'][-1]['attributes']['Datum_neu']/1000, tz=datetime.timezone.utc) # date from last entry in data set
    try:
        cached_data_latest_date = datetime.datetime.fromtimestamp(cached_data['features'][-1]['attributes']['Datum_neu']/1000, tz=datetime.timezone.utc)
    except TypeError:
        cached_data_latest_date = datetime.datetime(1970, 1, 1) # use default date if no cached data is available
    # check whether downloaded JSON contains new data or user enforced data collection 
    if data == cached_data and not args.force_collect:
        logger.info('Data has not changed.')
    else:
        if data != cached_data:
            # check whether data contains a new or updated day
            if data_latest_date >= midnight and data_latest_date != cached_data_latest_date:
                data_change = 'added'
                logger.info('New data for today has been found!')
            elif data_latest_date >= midnight and data_latest_date == cached_data_latest_date:
                data_change = 'updated'
                logger.info('Updated data for today has been found!')
            elif data_latest_date < midnight and data_latest_date != cached_data_latest_date:
                data_change = 'added'
                logger.info('New data for a previous day has been found!')
            elif data_latest_date < midnight and data_latest_date == cached_data_latest_date:
                data_change = 'updated'
                logger.info('Updated data for a previous day has been found!')
        else:
            # '--force-collect'
            data_change = 'updated'
            if data_latest_date >= midnight:
                logger.info('Data for today has not been changed, but is nevertheless collected as requested.')
            else:
                logger.info('Data for a previous day has not been changed, but is nevertheless collected as requested.')

        # save query date
        if args.date:
            try:
                data_load_date = dateutil.parser.parse(args.date) # use user's publishing date if given for the new data set
            except dateutil.parser.ParserError:
                logger.error('Failed to parse publishing date \'{:s}\'.'.format(args.date))
                sys.exit()
        elif args.file and not args.auto_date:
            json_filename = pathlib.Path(args.file.name).stem
            try:
                data_load_date = dateutil.parser.parse(json_filename) # try to parse the filename as date, if '--auto-date' option is set or data is loaded downloaded from server
            except dateutil.parser.ParserError:
                logger.error('Failed to parse the publishing date \'{:s}\' from filename. Please rename the file so that it has a valid date format or use the \'--date\' option to specify the date or pass \'--auto-date\' to save the current time as the publishing date for this time series. For further help type \'python {} --help\'.'.format(json_filename, pathlib.Path(__file__).resolve().name))
                sys.exit()
        #else: # loaded from file with '--auto-date' option or downloaded from server

        # cache JSON file
        if not args.no_cache:
            with open(json_file_path, 'w') as json_file:
                json.dump(data, json_file, indent=2)
        # archive JSON file
        if args.archive_json:
            # Save JSON data in different styles
            json_styles = {
                #'subfolder'    : JSON indent parameter; https://stackabuse.com/reading-and-writing-json-to-a-file-in-python/
                'json'          : None, # save JSON without line breaks and indentation for better processing performance; https://geobern.blogspot.com/2017/02/the-difference-between-json-and-pjson.html
                'pjson'         : 2, # make JSON human readable by pretty printing
            }
            for folder, indent in json_styles.items():
                archive_file_dir = pathlib.Path(abs_python_file_dir, JSON_ARCHIVE_FOLDER, folder)
                pathlib.Path.mkdir(archive_file_dir, exist_ok=True)
                archive_file_path = pathlib.Path(archive_file_dir, '{:s}.json'.format(data_load_date.strftime('%Y-%m-%dT%H:%M:%SZ')))
                with open(archive_file_path, 'w') as json_file:
                    json.dump(data, json_file, indent=indent)

        if args.skip_influxdb:
            logger.info('Skipping writing to InfluxDB.')
            sys.exit()

        # define tags of the time series
        influxdb_pub_date = data_load_date # date on which the record was published
        influxdb_tag_latest_date_short = data_latest_date # shorter version for graph legend aliases in Grafana; https://grafana.com/docs/grafana/latest/datasources/influxdb/#alias-patterns
        influxdb_field_latest_date = data_latest_date
        influxdb_tag_script_version = RELEASE # state version number of this script

        # generate time series list according to the expected InfluxDB line protocol: https://docs.influxdata.com/influxdb/v1.8/write_protocols/line_protocol_tutorial/
        for influx_db_measurement in INFLUXDB_MEASUREMENTS:
            time_series = []
            time_series_2 = []
            new_time_series_corrected_total = []
            for point in data['features']:
                #if influx_db_measurement == 'dresden_official_shifted':
                #    time = dateutil.parser.parse(point['attributes']['Datum'], dayfirst=True).replace(tzinfo=datetime.timezone.utc) + datetime.timedelta(days=1)
                #else:
                #   time = dateutil.parser.parse(point['attributes']['Datum'], dayfirst=True).replace(tzinfo=datetime.timezone.utc)
                time = dateutil.parser.parse(point['attributes']['Datum'], dayfirst=True).replace(tzinfo=datetime.timezone.utc)
                point_dict = {
                    'measurement'   : influx_db_measurement,
                    'tags'          : { # metadata for the data point
                        '01_latest_date_short_ymd'  : influxdb_tag_latest_date_short.strftime('%Y-%m-%d'), # other date format that is sorted correctly by InfluxDB; '01': display this tag first in InfluxDB queries
                        'latest_date_short'         : influxdb_tag_latest_date_short.strftime('%d.%m.%Y'), # more accurate name for the date used
                        'pub_date_short'            : influxdb_tag_latest_date_short.strftime('%d.%m.%Y'), # legacy name for the date of the latest time series entry, not actually the publishing date
                        'script_version'            : influxdb_tag_script_version,
                    },
                    'time'          : int(time.timestamp()), # parse date, switch month and day, explicetely set UTC (InfluxDB uses UTC), otherwise local timezone is assumed; 'datetime.datetime.isoformat()': generate ISO 8601 formatted string (e. g. '2020-10-22T21:30:13.883657+00:00')
                    'fields'        : { # in principle, a simple "point.pop('attributes')" also works, but unfortunately the field datatype is defined by the first point written to a series (in case of this foreign data set, some fields are filled with NoneType); https://github.com/influxdata/influxdb/issues/3460#issuecomment-124747104
                        # own fields
                        'pub_date'                      : influxdb_pub_date.strftime('%Y-%m-%dT%H:%M:%S'),
                        'pub_date_seconds'              : int(influxdb_field_latest_date.timestamp()), # legacy name, same as 'latest_date_seconds'
                        'latest_date_seconds'           : int(influxdb_field_latest_date.timestamp()), # add better searchable UNIX timestamp in seconds in addition to the human readable 'latest_date_short' tag; https://docs.influxdata.com/influxdb/v2.0/reference/glossary/#unix-timestamp; POSIX timestamps in Python: https://stackoverflow.com/a/8778548/7192373
                        'Meldedatum_or_Zuwachs'         : int(point['attributes'].get('Fälle_Meldedatum', point['attributes']['Zuwachs_Fallzahl']) or 0), # Get the field 'Fälle_Meldedatum' that was introduced by the city on 29.10.2020, for older data sets use the field 'Zuwachs_Fallzahl'
                        # fields from data source
                        'Anzeige_Indikator'             : str(point['attributes']['Anzeige_Indikator']), # value is either None or 'x'
                        'BelegteBetten'                 : int(point['attributes']['BelegteBetten'] or 0), # replace NoneType with 0
                        'Datum'                         : str(point['attributes']['Datum']),
                        'Datum_neu'                     : int(point['attributes']['Datum_neu'] or 0),
                        'Fallzahl'                      : int(point['attributes']['Fallzahl'] or 0),
                        'Fälle_Meldedatum'              : int(point['attributes'].get('Fälle_Meldedatum') or 0),
                        'Genesungsfall'                 : int(point['attributes']['Genesungsfall'] or 0),
                        'Hospitalisierung'              : int(point['attributes']['Hospitalisierung'] or 0),
                        'Hosp_Meldedatum'               : int(point['attributes'].get('Hosp_Meldedatum') or 0),
                        'Inzidenz'                      : float(point['attributes']['Inzidenz'] or 0),
                        'ObjectId'                      : int(point['attributes']['ObjectId'] or 0),
                        'Sterbefall'                    : int(point['attributes']['Sterbefall'] or 0),
                        'SterbeF_Meldedatum'            : int(point['attributes'].get('SterbeF_Meldedatum') or 0),
                        'Zeitraum'                      : str(point['attributes'].get('Zeitraum')),
                        'Zuwachs_Fallzahl'              : int(point['attributes']['Zuwachs_Fallzahl'] or 0),
                        'Zuwachs_Genesung'              : int(point['attributes']['Zuwachs_Genesung'] or 0),
                        'Zuwachs_Krankenhauseinweisung' : int(point['attributes']['Zuwachs_Krankenhauseinweisung'] or 0),
                        'Zuwachs_Sterbefall'            : int(point['attributes']['Zuwachs_Sterbefall'] or 0),
                    },
                }

                if influx_db_measurement == INFLUXDB_MEASUREMENT_ARCHIVE:
                    # save every time series, including all corrections of the city of the same day, in an separate InfluxDB measurement, distiguishable by a 'pub_date' tag (containing exact date and time)
                    point_dict['tags']['pub_date'] = influxdb_pub_date.strftime('%Y-%m-%dT%H:%M:%SZ')
                time_series.append(point_dict)

                # backdated processed cases 0-24 o'clock; save in point_dict2/time_series2
                previous_day = time - datetime.timedelta(days=1)
                total_cases_reporting_date = {
                    'time'      : int(previous_day.timestamp()),
                    'fields'    : {
                        'Fallzahl_Meldedatum'   : point_dict['fields']['Fallzahl'] - point_dict['fields']['Meldedatum_or_Zuwachs'], # calculate the actual number of cases without the report of the following day by 12 noon
                    },
                }
                new_time_series_corrected_total.append(total_cases_reporting_date) # save also for later for the "python" measurement
                # copy over point_dict and overwrite 'time' and 'fields' (preserve tags)
                point_dict2 = copy.deepcopy(point_dict)
                point_dict2.update(total_cases_reporting_date) # take the old dict as a template and overwrite fields with only this single field
                time_series_2.append(point_dict2)

            # add today's reported cases (until 12 o'clock)
            total_cases_reporting_date = {
                'time'      : int(time.timestamp()),
                'fields'    : {
                    'Fallzahl_Meldedatum'   : point_dict['fields']['Fallzahl'],
                },
            }
            point_dict2 = copy.deepcopy(point_dict) # copy last point_dict of the 'for' loop
            point_dict2.update(total_cases_reporting_date) # overwrite dict with 'time' and 'fields'
            time_series_2.append(point_dict2)

            # write data to database
            db_client.write_points(time_series, time_precision='s')
            db_client.write_points(time_series_2, time_precision='s')

        # do own calculations
        # measurement that contains the daily 12 pm reports (last point of each day)
        new_time_series_point = time_series[-1]
        # convert some tags into fields (dict depth = 1)
        field_changes = {
            '01_latest_date_short_ymd'  : new_time_series_point['tags']['01_latest_date_short_ymd'],
            'pub_date_short'            : new_time_series_point['tags']['latest_date_short'], # legacy name for compatibility reasons
            'latest_date_short'         : new_time_series_point['tags']['latest_date_short'], # more accurate name
            'Fallzahl_Meldedatum'       : new_time_series_point['fields']['Fallzahl'] # add today's reported cases (until 12 o'clock)
        }
        new_time_series_point['fields'].update(field_changes)
        # replace measurement name and tags (dict depth = 0)
        new_time_series_point_metadata = {
            'measurement'   : 'python',
            'tags'          : {
                'data_version'  : 'noon',
            },
        }
        new_time_series_point.update(new_time_series_point_metadata) # add metadata
        db_client.write_points([new_time_series_point], time_precision='s')

        new_time_series_corrected_total_previous_day = new_time_series_corrected_total[-1]
        new_time_series_corrected_total_previous_day.update(new_time_series_point_metadata)
        db_client.write_points([new_time_series_corrected_total_previous_day], time_precision='s')

        series_key = 'latest_date_short={:s},script_version={:s}'.format(influxdb_tag_latest_date_short.strftime('%d.%m.%Y'), influxdb_tag_script_version) # https://docs.influxdata.com/influxdb/v1.8/concepts/glossary/#series-key

        if data_change == 'added':
            logger.info('Time series with tags \'{:s}\' successfully added to database.'.format(series_key))
        elif data_change == 'updated':
            logger.info('Time series with tags \'{:s}\' successfully updated in database.'.format(series_key))

if __name__ == '__main__':
    main()

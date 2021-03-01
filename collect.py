#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# constants
RELEASE = 'v0.3.0'
ARCGIS_JSON_URL = 'https://services.arcgis.com/ORpvigFPJUhb8RDF/arcgis/rest/services/corona_DD_7_Sicht/FeatureServer/0/query?f=pjson&where=ObjectId>=0&outFields=*'
GITHUB_JSON_URL = 'https://raw.githubusercontent.com/jdieg0/coronavirus-dresden-data/main/latest-json' # points to a JSON file that has been checked by maintainers for errors committed by the city
CACHED_JSON_FILENAME = 'cached.json'
OUTPUT_FOLDER = 'output'
JSON_ARCHIVE_FOLDER = 'data'

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
    # derive log file name from script name
    log_filename = '{}{:s}'.format(pathlib.Path(__file__).resolve().stem, '.log')

    # read command line arguments (https://docs.python.org/3/howto/argparse.html)
    argparser = argparse.ArgumentParser(description='Collects official SARS-CoV-2 infection statistics published by the city of Dresden.')
    arg_group_inputs = argparser.add_argument_group('input options', 'by default, the data is obtained online from the city\'s official source, but other import options are also available')
    arg_group_timestamps = argparser.add_mutually_exclusive_group()
    arg_group_outputs = argparser.add_argument_group('output options', 'new data is saved in InfluxDB by default; this and other behaviour concerning data writing can be adjusted with these output options')
    arg_group_outputs.add_argument('-a', '--archive-json', help='archive JSON file each time new data is found or force-collected', action='store_true')
    argparser.add_argument('-c', '--force-collect', help='store JSON data, regardless of whether new data points have been found or not', action='store_true')
    arg_group_timestamps.add_argument('-d', '--date', help='set publishing date manually for the new data set, e. g. \'2020-10-18T09:52:41Z\'')
    arg_group_inputs.add_argument('-f', '--file', help='load JSON data from a local file instead from server; if no publishing date is passed with the \'--date\' or \'--auto-date\' option, an attempt is made to read the date from the filename', nargs='?', type=argparse.FileType('r'), const='query.json') # 'const' is used, if '--file' is passed without an argument; default=sys.stdin; https://stackoverflow.com/a/15301183/7192373
    arg_group_outputs.add_argument('-l', '--log', help='save log in file \'{:s}\''.format(log_filename), action='store_true')
    arg_group_outputs.add_argument('-n', '--no-cache', help='suppress the saving of a JSON cache file (helpful if you do not want to mess with an active cron job looking for changes)', action='store_true')
    arg_group_outputs.add_argument('-o', '--output-dir', help='set a user defined directory where data (cache, logs and JSONs) are stored; default: directory of this Python script', default=pathlib.Path(pathlib.Path(__file__).resolve().parent, OUTPUT_FOLDER)) # use absolute path of this Python folder as default directory
    arg_group_outputs.add_argument('-s', '--skip-influxdb', help='check for and write new JSON data only, do not write to InfluxDB', action='store_true')
    arg_group_timestamps.add_argument('-t', '--auto-date', help='do not try to to parse the publishing date from the filename, instead write current date (UTC) to database', action='store_true')
    arg_group_inputs.add_argument('-u', '--url', help='URL to be used to check for JSON updates; default: \'arcgis\'', choices=['arcgis', 'github'], default='arcgis', type=str.lower)
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
    logging_format = '[%(asctime)s] %(levelname)s %(message)s' # %(name)s.%(funcName)s %(pathname)s:
    log_formatter = logging.Formatter(logging_format) #, datefmt="%Y-%m-%dT%H:%M:%S")

    # log to console
    handler = logging.StreamHandler()
    handler.setFormatter(log_formatter)
    logger.addHandler(handler)

    # get path for output
    global output_dir
    try:
        output_dir = pathlib.Path(args.output_dir)
    except TypeError:
        logger.error(f'Could not resolve output directory \'{args.output_dir}\'.')
        sys.exit()

    # log to file
    if args.log:
        handler = logging.handlers.RotatingFileHandler(pathlib.Path(output_dir, log_filename), maxBytes=2**20, backupCount=5) # https://stackoverflow.com/a/13733777/7192373; https://docs.python.org/3/library/logging.handlers.html#logging.handlers.RotatingFileHandler
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
    cached_json_path = pathlib.Path(output_dir, CACHED_JSON_FILENAME).resolve()
    try:
        with open(cached_json_path, 'r') as json_file:
            cached_data = json.load(json_file)
    except FileNotFoundError:
        cached_data = None
        logger.debug('File \'{:s}\' not found.'.format(CACHED_JSON_FILENAME))

    # load (possibly) new JSON data and write it to InfluxDB
    if args.file:
        data = json.load(args.file)
        logger.debug('Read JSON data from local file \'{:s}\'.'.format(args.file.name))
    else:
        # choose right URL to JSON file
        if args.url == 'arcgis':
            json_url = ARCGIS_JSON_URL
            with urllib.request.urlopen(json_url) as response:
                data = json.load(response)
                logger.debug(f'Downloaded JSON data from server \'ArcGIS\'.')
        else:
            symlink_url = GITHUB_JSON_URL
            # read relative path to latest JSON
            with urllib.request.urlopen(symlink_url) as response:
                symlink = response.read().decode('utf-8')
                json_url = urllib.parse.urljoin(symlink_url, symlink) # put together the full JSON URL
            # open the JSON itself
            with urllib.request.urlopen(json_url) as response:
                data = json.load(response)
                logger.debug(f'Downloaded JSON data from server \'GitHub\'.')
   
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
        elif (args.file or args.url == 'github') and not args.auto_date:
            if args.file:
                json_filename = pathlib.Path(args.file.name).stem # read date from name of local file
            else:
                json_filename = pathlib.Path(json_url.rsplit('/', 1)[-1]).stem # read date from name of linked file on GitHub
            
            try:
                data_load_date = dateutil.parser.parse(json_filename) # try to parse the filename as date, if '--auto-date' option is set or data is loaded downloaded from server
            except dateutil.parser.ParserError:
                logger.error('Failed to parse the publishing date \'{:s}\' from filename. Please rename the file so that it has a valid date format or use the \'--date\' option to specify the date or pass \'--auto-date\' to save the current time as the publishing date for this time series. For further help type \'python {} --help\'.'.format(json_filename, pathlib.Path(__file__).resolve().name))
                sys.exit()
        # else:
            # loaded from file with '--auto-date' option or downloaded from ArcGIS server

        # cache JSON file
        if not args.no_cache:
            pathlib.Path.mkdir(output_dir, parents=True, exist_ok=True)
            with open(cached_json_path, 'w') as json_file:
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
                archive_file_dir = pathlib.Path(output_dir, JSON_ARCHIVE_FOLDER, folder)
                pathlib.Path.mkdir(archive_file_dir, parents=True, exist_ok=True)
                archive_file_path = pathlib.Path(archive_file_dir, '{:s}.json'.format(data_load_date.strftime('%Y-%m-%dT%H%M%SZ')))
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

            # 'python' measurement
            python_measurement_metadata = {
                'measurement'   : 'python',
                'tags'          : {
                    'data_version'  : 'noon',
                    },
            }
            cases_processed_series = []
            cases_reported_series = []

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
                        'Anzeige_Indikator'             : str(point['attributes'].get('Anzeige_Indikator')), # value is either None or 'x'
                        'BelegteBetten'                 : int(point['attributes'].get('BelegteBetten') or 0), # replace NoneType with 0
                        'Datum'                         : str(point['attributes'].get('Datum')),
                        'Datum_neu'                     : int(point['attributes'].get('Datum_neu') or 0),
                        'Fallzahl'                      : int(point['attributes'].get('Fallzahl') or 0),
                        'Fallzahl_aktiv'                : int(point['attributes'].get('Fallzahl_aktiv') or 0),
                        'Fallzahl_aktiv_Zuwachs'        : int(point['attributes'].get('Fallzahl_aktiv_Zuwachs') or 0),
                        'Fälle_Meldedatum'              : int(point['attributes'].get('Fälle_Meldedatum') or 0),
                        'Genesungsfall'                 : int(point['attributes'].get('Genesungsfall') or 0),
                        'Hospitalisierung'              : int(point['attributes'].get('Hospitalisierung') or 0),
                        'Hosp_Meldedatum'               : int(point['attributes'].get('Hosp_Meldedatum') or 0),
                        'Inzidenz'                      : float(point['attributes'].get('Inzidenz') or 0),
                        'Inzi_SN_RKI'                   : float(point['attributes'].get('Inzi_SN_RKI')) if point['attributes'].get('Inzi_SN_RKI') else None,
                        'Inzidenz_RKI'                  : float(point['attributes'].get('Inzidenz_RKI')) if point['attributes'].get('Inzidenz_RKI') else None, # convert only to float if value is not None
                        'Krh_I'                         : int(point['attributes'].get('Krh_I') or 0),
                        'Krh_I_belegt'                  : int(point['attributes'].get('Krh_I_belegt') or 0),
                        'Krh_I_covid'                   : int(point['attributes'].get('Krh_I_covid') or 0),
                        'Krh_I_frei'                    : int(point['attributes'].get('Krh_I_frei') or 0),
                        'Krh_N'                         : int(point['attributes'].get('Krh_N') or 0),
                        'Krh_N_belegt'                  : int(point['attributes'].get('Krh_N_belegt') or 0),
                        'Krh_N_frei'                    : int(point['attributes'].get('Krh_N_frei') or 0),
                        'Mutation'                      : int(point['attributes'].get('Mutation')) if point['attributes'].get('Mutation') else None,
                        'ObjectId'                      : int(point['attributes'].get('ObjectId') or 0),
                        'Sterbefall'                    : int(point['attributes'].get('Sterbefall') or 0),
                        'SterbeF_Meldedatum'            : int(point['attributes'].get('SterbeF_Meldedatum') or 0),
                        'SterbeF_Sterbedatum'           : int(point['attributes'].get('SterbeF_Sterbedatum') or 0),
                        'Vorz_akt_Faelle'               : str(point['attributes'].get('Vorz_akt_Faelle')),
                        'Zeitraum'                      : str(point['attributes'].get('Zeitraum')),
                        'Zuwachs_Fallzahl'              : int(point['attributes'].get('Zuwachs_Fallzahl') or 0),
                        'Zuwachs_Genesung'              : int(point['attributes'].get('Zuwachs_Genesung') or 0),
                        'Zuwachs_Krankenhauseinweisung' : int(point['attributes'].get('Zuwachs_Krankenhauseinweisung') or 0),
                        'Zuwachs_Mutation'              : int(point['attributes'].get('Zuwachs_Mutation')) if point['attributes'].get('Zuwachs_Mutation') else None,
                        'Zuwachs_Sterbefall'            : int(point['attributes'].get('Zuwachs_Sterbefall') or 0),
                    },
                }

                if influx_db_measurement == INFLUXDB_MEASUREMENT_ARCHIVE:
                    # save every time series, including all corrections of the city of the same day, in an separate InfluxDB measurement, distiguishable by a 'pub_date' tag (containing exact date and time)
                    point_dict['tags']['pub_date'] = influxdb_pub_date.strftime('%Y-%m-%dT%H:%M:%SZ')
                
                # save point to time series/measurement
                time_series.append(point_dict)

                # backdated processed cases 0-24 o'clock; save in point_dict2/time_series2
                previous_day = time - datetime.timedelta(days=1)
                cases_processed_by_date = {
                    'time'      : int(previous_day.timestamp()),
                    'fields'    : {
                        'Fallzahl_Meldedatum'                   : point_dict['fields']['Fallzahl'] - point_dict['fields']['Meldedatum_or_Zuwachs'], # calculate the actual number of cases without the report of the following day by 12 noon
                    },
                }

                # copy point_dict and overwrite 'time' and 'fields' (preserve tags)
                point_dict2 = copy.deepcopy(point_dict)
                point_dict2.update(cases_processed_by_date) # take the old dict as a template and overwrite fields with only this single field
                time_series_2.append(point_dict2)

                # save also for later for the 'python' measurement
                cases_processed_series.append(cases_processed_by_date) # 'Fallzahl_Meldedatum' minus today's cases shifted to yesterday
                # 'Fallzahl_Meldedatum' column only
                field_changes =  {
                    'fields'    : {
                        'Meldedatum_or_Zuwachs_zuletzt_importiert'  : point_dict['fields']['Meldedatum_or_Zuwachs'],
                    },
                }
                cases_reported_by_date = copy.deepcopy(point_dict)
                cases_reported_by_date.update(python_measurement_metadata)
                cases_reported_by_date.update(field_changes)
                cases_reported_series.append(cases_reported_by_date)

            # add today's reported cases (until 12 o'clock)
            cases_processed_by_date = {
                'time'      : int(time.timestamp()),
                'fields'    : {
                    'Fallzahl_Meldedatum'   : point_dict['fields']['Fallzahl'],
                },
            }
            point_dict2 = copy.deepcopy(point_dict) # copy last point_dict of the 'for' loop
            point_dict2.update(cases_processed_by_date) # overwrite dict with 'time' and 'fields'
            time_series_2.append(point_dict2)

            # write data to database
            db_client.write_points(time_series, time_precision='s')
            db_client.write_points(time_series_2, time_precision='s')

        # do own calculations
        # measurement that contains the daily 12 pm reports (last point of each day)
        time_series_latest = time_series[-1]
        # convert some tags into fields (dict depth = 1)
        field_changes = {
            '01_latest_date_short_ymd'  : time_series_latest['tags']['01_latest_date_short_ymd'],
            'pub_date_short'            : time_series_latest['tags']['latest_date_short'], # legacy name for compatibility reasons
            'latest_date_short'         : time_series_latest['tags']['latest_date_short'], # more accurate name
            'Fallzahl_Meldedatum'       : time_series_latest['fields']['Fallzahl'] # add today's reported cases (until 12 o'clock)
        }
        time_series_latest['fields'].update(field_changes)
        # replace measurement name and tags (dict depth = 0)
        time_series_latest.update(python_measurement_metadata) # add metadata
        db_client.write_points([time_series_latest], time_precision='s')

        # add value for the reported cases of the data set published on the following day (so that the public health office had 36 h time to count them instead of 12 h)
        point_day_before = time_series[-2]
        point_day_before.update(python_measurement_metadata)
        fields_overwrite = {
            'fields'    : {
                'Fälle_Meldedatum_Datenstand_Folgetag'  : point_day_before['fields']['Meldedatum_or_Zuwachs']
            },
        }
        point_day_before.update(fields_overwrite)
        db_client.write_points([point_day_before], time_precision='s')
        
        # 'Fallzahl_Meldedatum' minus today's cases shifted to yesterday
        cases_processed_series_previous_day = cases_processed_series[-1]
        cases_processed_series_previous_day.update(python_measurement_metadata)
        db_client.write_points([cases_processed_series_previous_day], time_precision='s')

        db_client.write_points(cases_reported_series, time_precision='s')

        series_key = 'latest_date_short={:s},script_version={:s}'.format(influxdb_tag_latest_date_short.strftime('%d.%m.%Y'), influxdb_tag_script_version) # https://docs.influxdata.com/influxdb/v1.8/concepts/glossary/#series-key

        if data_change == 'added':
            logger.info('Time series with tags \'{:s}\' successfully added to database.'.format(series_key))
        elif data_change == 'updated':
            logger.info('Time series with tags \'{:s}\' successfully updated in database.'.format(series_key))

if __name__ == '__main__':
    main()

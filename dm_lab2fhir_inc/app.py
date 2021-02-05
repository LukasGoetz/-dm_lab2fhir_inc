#!/usr/bin/python3.6

import sys
import os
import datetime
import configparser
import logging
import argparse
from lib import umm_on_fhir, umm_db_lib

def is_valid_file(arg):
  if not os.path.exists(arg):
    raise FileNotFoundError(arg)
  return arg

def main():
  try:
    return_val = 0
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s [%(levelname)s] %(message)s",
                        handlers=[logging.FileHandler('debug.log'),
                                  logging.StreamHandler()])
    logger = logging.getLogger(__name__)

    parser = argparse.ArgumentParser()
    parser.add_argument('-s', '--start_date', dest='start_date', help='start date of admission',
                        type=lambda s: datetime.datetime.strptime(s, '%Y-%m-%d').date(), required=True)
    parser.add_argument('-e', '--end_date', dest='end_date', help='end date of admission',
                        type=lambda s: datetime.datetime.strptime(s, '%Y-%m-%d').date(), required=True)
    parser.add_argument('-c', '--path_to_config', dest='path_to_config', help='path to config',
                        required=True, metavar="FILE", type=lambda x: is_valid_file(x))
    parser.add_argument('-d', '--dest', dest='dest_type', help='where to store the FHIR records',
                        required=True, type=str, choices=['psql', 'hapi'])
    parser.add_argument('-n', '--no_lab', dest='lab', help='exclude lab data for mapping',
                        action='store_false')
    args = parser.parse_args()

    config = configparser.ConfigParser()
    config.read(args.path_to_config)


    db_con_dwh_raw = umm_db_lib.DBConnectionUMM(config, 'dwh_db', logger)
    db_con_fhir_raw = umm_db_lib.DBConnectionUMM(config, 'fhir_db', logger)
    db_con_fhir = db_con_fhir_raw.get_engine()
    db_con_dwh = db_con_dwh_raw.create_con()
    new_period = umm_on_fhir.UMMPeriod(args.start_date, args.end_date)
    if args.dest_type == 'psql':
      new_dest = umm_on_fhir.UMMDestination('psql', db_con_fhir)
    else:
      new_dest = umm_on_fhir.UMMDestination('hapi', config['server']['url_hapi_fhir'])

    new_processor = umm_on_fhir.UMMonFHIR(config, logger)
    # process patient records
    res_stats = new_processor.process_patients(new_period, db_con_dwh, new_dest, True)

    # process encounter records
    res_stats = new_processor.process_encounters(new_period, db_con_dwh, new_dest, True)

    # process transfer records
    res_stats = new_processor.process_transfers(new_period, db_con_dwh, new_dest, True)

    # process conditions
    # todo ignore conditions where encounter does not exist
    res_stats = new_processor.process_conditions(new_period, db_con_dwh, new_dest, True)

    # process procedures/ medication/ medicationstatements
    res_stats = new_processor.process_procedures(new_period, db_con_dwh, new_dest, True)

    # process lufu records
    #res_stats = new_processor._process_lufu(new_period, db_con_dwh, new_dest, True)

    # process lab records
    if args.lab:
      res_stats = new_processor.process_lab_results(new_period, db_con_dwh, new_dest, True)

    logger.info("ETL job was successfully completed")


  except FileNotFoundError as exc:
    return_val = 1
    logger.error(f"In {__name__}: File '{exc}' does not exist", exc_info=True)
  except Exception as exc:
    return_val = 1
    logger.error(f"In '{__name__}': Unexpected error 'exc' occurred", exc_info=True)
  except SystemExit:
    return_val = 1
  finally:
    sys.exit(return_val)

if __name__ == "__main__":
  main()

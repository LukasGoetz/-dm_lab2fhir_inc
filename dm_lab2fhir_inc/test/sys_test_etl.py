#!/usr/bin/python3.6

'''Perform system tests
   Arguments: app (application name), cfile (config)
   Returns: none
   Authors: Lukas Goetz, Lukas.Goetz@medma.uni-heidelberg.de
   Date: 01-04-2021'''

import subprocess, sys
import pytest, logging, configparser
from lib import umm_db_lib

@pytest.fixture()
def app(pytestconfig):
  return pytestconfig.getoption("app")

@pytest.fixture()
def cfile(pytestconfig):
  return pytestconfig.getoption("cfile")

def test(app, cfile):
  logging.basicConfig(level=logging.INFO,
                      format="%(asctime)s [%(levelname)s] %(message)s",
                      handlers=[logging.FileHandler('debug.log'),
                                logging.StreamHandler()])
  logger = logging.getLogger(__name__)

  start_date = '2000-01-01'
  end_date = '2020-12-12'
  logger.info("Step: Preparation")
  logger.info(f"Action: Test data from {start_date} to {end_date} are being mapped to HL7 FHIR")
  process = subprocess.run([sys.executable, app, '-s', start_date, '-e', end_date,
                            '-c', cfile, '-d', 'psql'])
  assert(process.returncode == 0)


  logger.info("Step: Data completeness")
  logger.info("Action: Check if all test records of all types "\
              "were completely transferred to HL7 FHIR")
  logger.info("Expected Result: Return value should be 'PASSED'")
  config = configparser.ConfigParser()
  config.read(cfile)
  db_con = umm_db_lib.DBConnectionUMM(config, 'fhir_db', logger)
  db_con_fhir = db_con.get_engine()
  try:
    nof_ups, nof_rm = umm_db_lib.get_ups_rm_num_fhir('Patient', '2000-01-01', db_con_fhir)
    assert(nof_ups == 20 and nof_rm == 0)
    nof_ups, nof_rm = umm_db_lib.get_ups_rm_num_fhir('Encounter', '2000-01-01', db_con_fhir)
    assert(nof_ups == 31 and nof_rm == 0)
    nof_ups, nof_rm = umm_db_lib.get_ups_rm_num_fhir('Condition', '2000-01-01', db_con_fhir)
    assert(nof_ups == 20 and nof_rm == 0)
    nof_ups, nof_rm = umm_db_lib.get_ups_rm_num_fhir('Procedure', '2000-01-01', db_con_fhir)
    assert(nof_ups == 20 and nof_rm == 0)
    logger.info("Actual Result: 'PASSED'")
  except AssertionError as exc:
    logger.error("Actual Result: 'FAILED'")
    raise


  logger.info("Step: Data integrity")
  logger.info("Action: Check if references in HL7 FHIR records of "\
              "all types are set properly")
  logger.info("Expected Result: Return value should be 'PASSED'")
  try:
    nrows = umm_db_lib.get_pat_num_fhir('Encounter', db_con_fhir)
    assert(nrows == 19)
    nrows = umm_db_lib.get_pat_num_fhir('Condition', db_con_fhir)
    assert(nrows == 6)
    nrows = umm_db_lib.get_pat_num_fhir('Procedure', db_con_fhir)
    assert(nrows == 6)
    nrows = umm_db_lib.get_case_num_fhir('Condition', db_con_fhir)
    assert(nrows == 7)
    nrows = umm_db_lib.get_case_num_fhir('Procedure', db_con_fhir)
    assert(nrows == 6)
    logger.info("Actual Result: 'PASSED'")
  except AssertionError as exc:
    logger.error("Actual Result: 'FAILED'")
    raise


  start_date = '2020-12-12'
  end_date = '2020-12-14'
  logger.info("Step: Preparation")
  logger.info(f"Action: Test data from {start_date} to {end_date} are being mapped to HL7 FHIR")
  process = subprocess.run([sys.executable, app, '-s', start_date, '-e', end_date,
                            '-c', cfile, '-d', 'psql'])
  assert(process.returncode == 0)


  logger.info("Step: Data completeness")
  logger.info("Action: Check if all test records of all types"\
              " were completely transferred to HL7 FHIR")
  try:
    nof_ups, nof_rm = umm_db_lib.get_ups_rm_num_fhir('Patient', '2000-01-01', db_con_fhir)
    assert(nof_ups == 19 and nof_rm == 1)
    nof_ups, nof_rm = umm_db_lib.get_ups_rm_num_fhir('Encounter', '2000-01-01', db_con_fhir)
    assert(nof_ups == 31 and nof_rm == 0)
    nof_ups, nof_rm = umm_db_lib.get_ups_rm_num_fhir('Condition', '2000-01-01', db_con_fhir)
    assert(nof_ups == 20 and nof_rm == 0)
    nof_ups, nof_rm = umm_db_lib.get_ups_rm_num_fhir('Procedure', '2000-01-01', db_con_fhir)
    assert(nof_ups == 20 and nof_rm == 0)
    logger.info("Actual Result: 'PASSED'")
  except AssertionError as exc:
    logger.error("Actual Result: 'FAILED'")
    raise


  logger.info("Step: Data integrity")
  logger.info("Action: Check if references in HL7 FHIR records of"\
              " all types are set properly")
  logger.info("Expected Result: Return value should be 'PASSED'")
  try:
    nrows = umm_db_lib.get_pat_num_fhir('Encounter', db_con_fhir)
    assert(nrows == 19)
    nrows = umm_db_lib.get_pat_num_fhir('Condition', db_con_fhir)
    assert(nrows == 6)
    nrows = umm_db_lib.get_pat_num_fhir('Procedure', db_con_fhir)
    assert(nrows == 6)
    nrows = umm_db_lib.get_case_num_fhir('Condition', db_con_fhir)
    assert(nrows == 7)
    nrows = umm_db_lib.get_case_num_fhir('Procedure', db_con_fhir)
    assert(nrows == 6)
    logger.info("Actual Result: 'PASSED'")
  except AssertionError as exc:
    logger.error("Actual Result: 'FAILED'")
    raise

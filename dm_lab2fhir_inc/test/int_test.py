#!/usr/bin/python3.6

'''Perform integration tests
   Arguments: cfile (config)
   Returns: none
   Authors: Lukas Goetz, Lukas.Goetz@medma.uni-heidelberg.de
   Date: 01-04-2021'''

import subprocess, sys, unittest
import pytest, logging, configparser
from lib import umm_on_fhir, umm_db_lib

@pytest.fixture()
def cfile(pytestconfig):
  return pytestconfig.getoption("cfile")

def test(cfile):
  logging.basicConfig(level=logging.INFO,
                      format="%(asctime)s [%(levelname)s] %(message)s",
                      handlers=[logging.FileHandler('debug.log'),
                                logging.StreamHandler()])
  logger = logging.getLogger(__name__)
  config = configparser.ConfigParser()
  config.read(cfile)
  new_ummonfhir = umm_on_fhir.UMMonFHIR(config, logger)

  db_con_fhir_raw = umm_db_lib.DBConnectionUMM(config, 'fhir_db', logger)
  db_con_fhir = db_con_fhir_raw.get_engine()
  db_con_dwh_raw = umm_db_lib.DBConnectionUMM(config, 'dwh_db', logger)
  db_con_dwh = db_con_dwh_raw.create_con()

  new_period = umm_on_fhir.UMMPeriod('2000-01-01', '2020-12-12')
  new_dest = umm_on_fhir.UMMDestination('psql', db_con_fhir)

  logger.info("Step: Positive test batch patient FHIR mapping")
  logger.info("Action: Map batch of patient records to FHIR, validate and "
              "check if all records could be sucessfully mapped and validated")
  logger.info("Expected Result: Return value should be 'PASSED'")
  res_stats = new_ummonfhir.process_patients(new_period, db_con_dwh, new_dest, False)
  try:
    assert(res_stats['valid_pat'] == 20 and res_stats['invalid_pat'] == 0)
    assert(res_stats['rm_req_pat'] == 0)
    assert(res_stats['ups_pat'] == 20 and res_stats['rm_pat'] == 0)
    logger.info("Actual Result: PASSED")
  except AssertionError as exc:
    logger.error("Actual Result: FAILED")
    raise

  logger.info("Step: Positive test batch encounter FHIR mapping")
  logger.info("Action: Map batch of encounter records to FHIR, validate and "
              "check if all records could be sucessfully mapped and validated")
  logger.info("Expected Result: Return value should be 'PASSED'")
  res_stats = new_ummonfhir.process_encounters(new_period, db_con_dwh, new_dest, False)
  try:
    assert(res_stats['valid_con'] == 20 and res_stats['invalid_con'] == 0)
    assert(res_stats['valid_enc'] == 20 and res_stats['invalid_enc'] == 0)
    assert(res_stats['valid_subenc'] == 11 and res_stats['invalid_subenc'] == 0)
    assert(res_stats['valid_loc'] == 20 and res_stats['invalid_loc'] == 0)
    assert(res_stats['valid_obs'] == 20 and res_stats['invalid_obs'] == 0)
    assert(res_stats['rm_req_enc'] == 0)
    assert(res_stats['ups_con'] == 20 and res_stats['rm_con'] == 0)
    assert(res_stats['ups_enc'] == 31 and res_stats['rm_enc'] == 0)
    assert(res_stats['ups_obs'] == 20 and res_stats['rm_obs'] == 0)
    logger.info("Actual Result: PASSED")
  except AssertionError as exc:
    logger.error("Actual Result: FAILED")
    raise


  logger.info("Step: Positive test batch diagnosis FHIR mapping")
  logger.info("Action: Map batch of diagnosis records to FHIR, validate and "
              "check if all records could be sucessfully mapped and validated")
  logger.info("Expected Result: Return value should be 'PASSED'")
  res_stats = new_ummonfhir.process_conditions(new_period, db_con_dwh, new_dest, False)
  try:
    assert(res_stats['valid_con'] == 20 and res_stats['invalid_con'] == 0)
    assert(res_stats['rm_req_con'] == 0)
    assert(res_stats['ups_con'] == 20 and res_stats['rm_con'] == 0)
    logger.info("Actual Result: PASSED")
  except AssertionError as exc:
    logger.error("Actual Result: FAILED")
    raise


  logger.info("Step: Positive test batch procedure FHIR mapping")
  logger.info("Action: Map batch of procedure records to FHIR, validate and "
              "check if all records could be sucessfully mapped and validated")
  logger.info("Expected Result: Return value should be 'PASSED'")
  res_stats = new_ummonfhir.process_procedures(new_period, db_con_dwh, new_dest, False)
  try:
    assert(res_stats['valid_prod'] == 20 and res_stats['invalid_prod'] == 0)
    assert(res_stats['valid_med'] == 2 and res_stats['invalid_med'] == 0)
    assert(res_stats['valid_medstm'] == 2 and res_stats['invalid_medstm'] == 0)
    assert(res_stats['rm_req_prod'] == 0)
    assert(res_stats['ups_prod'] == 20 and res_stats['rm_prod'] == 0)
    assert(res_stats['ups_med'] == 2 and res_stats['rm_med'] == 0)
    assert(res_stats['ups_medstm'] == 2 and res_stats['rm_medstm'] == 0)
    logger.info("Actual Result: PASSED")
  except AssertionError as exc:
    logger.error("Actual Result: FAILED")
    raise


  logger.info("Step: Positive test batch lab result FHIR mapping")
  logger.info("Action: Map batch of lab result records to FHIR, validate and "
              "check if all records could be sucessfully mapped and validated")
  logger.info("Expected Result: Return value should be 'PASSED'")
  res_stats = new_ummonfhir.process_lab_results(new_period, db_con_dwh, new_dest, False)
  try:
    assert(res_stats['valid_obs'] == 20 and res_stats['invalid_obs'] == 0)
    #assert res_stats['ups_obs'] == 20
    logger.info("Actual Result: PASSED")
  except AssertionError as exc:
    logger.error("Actual Result: FAILED")
    raise

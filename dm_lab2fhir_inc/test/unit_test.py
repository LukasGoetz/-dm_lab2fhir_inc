#!/usr/bin/python3.6

'''Perform unit tests
   Arguments: cfile (config)
   Returns: none
   Authors: Lukas Goetz, Lukas.Goetz@medma.uni-heidelberg.de
   Date: 01-04-2021'''

import subprocess, sys, unittest, pandas as pd, re, json
import pytest, logging, configparser
from lib import (umm_db_lib, mapper_dmpat2pat, mapper_dmenc2enc, mapper_dmdiag2cond,
                 mapper_dmpro2pro_med, mapper_dmlab2obs, pseudonymizer, fhir_bundle)
from lib.mii_profiles.fhirabstractbase import FHIRValidationError
from lib.mii_profiles import fhirreference, mii_patient

@pytest.fixture()
def cfile(pytestconfig):
  return pytestconfig.getoption("cfile")

def _test_patient_mapper(valid_record, invalid_record, logger, systems):
  patient_psn = 'dic-pid-110'
  logger.info("Step: Positive test patient FHIR mapping")
  logger.info("Action: Map valid patient record to FHIR and validate")
  logger.info("Expected Result: Return value should be 'PASSED'")
  new_mapper_dmpat2pat = mapper_dmpat2pat.MapperDMPat2Pat(logger, systems)
  new_mapper_dmpat2pat.read(patient_psn, valid_record)
  try:
    inpatient = new_mapper_dmpat2pat.map()
    print(inpatient.as_json())
    logger.info("Actual Result: PASSED")
  except Exception as exc:
    logger.error("Actual Result: FAILED")
    raise

  logger.info("Step: Negative test patient FHIR mapping")
  logger.info("Action: Map invalid patient record to FHIR and validate")
  logger.info("Expected Result: Return value should be 'FAILED'")
  new_mapper_dmpat2pat = mapper_dmpat2pat.MapperDMPat2Pat(logger, systems)
  new_mapper_dmpat2pat.read(patient_psn, invalid_record)
  with pytest.raises(FHIRValidationError):
    inpatient = new_mapper_dmpat2pat.map()
    inpatient.as_json()
    logger.error("Actual Result: PASSED")
  logger.info("Actual Result: FAILED")

def _test_encounter_mapper(valid_record, invalid_record, logger, systems, map_table):
  patient_psn = 'dic-pid-110'
  encounter_psn = 'dic-eid-110'
  ref = {'reference': f"Condition/fake_condition"}
  condition_ref = fhirreference.FHIRReference(jsondict=ref)
  ranked_cond_ref = [[condition_ref, 1]]
  logger.info("Step: Positive test encounter FHIR mapping")
  logger.info("Action: Map valid encounter record to FHIR and validate")
  logger.info("Expected Result: Return value should be 'PASSED'")
  new_mapper_dmenc2enc = mapper_dmenc2enc.MapperDMEnc2Enc(logger, systems,
                                                          map_table)
  new_mapper_dmenc2enc.read(ranked_cond_ref, patient_psn, encounter_psn, valid_record)
  try:
    inpatient = new_mapper_dmenc2enc.map()
    inpatient.as_json()
    logger.info("Actual Result: PASSED")
  except Exception as exc:
    logger.error("Actual Result: FAILED")
    raise

  logger.info("Step: Negative test encounter FHIR mapping")
  logger.info("Action: Map invalid encounter record to FHIR and validate")
  logger.info("Expected Result: Return value should be 'FAILED'")
  new_mapper_dmenc2enc = mapper_dmenc2enc.MapperDMEnc2Enc(logger, systems,
                                                          map_table)
  new_mapper_dmenc2enc.read(ranked_cond_ref, patient_psn, encounter_psn, invalid_record)
  with pytest.raises(FHIRValidationError):
    inpatient = new_mapper_dmenc2enc.map()
    inpatient.as_json()
    logger.error("Actual Result: PASSED")
  logger.info("Actual Result: FAILED")

def _test_condition_mapper(valid_record, invalid_record, logger, systems):
  patient_psn = 'dic-pid-110'
  encounter_psn = 'dic-eid-110'
  logger.info("Step: Positive test condition FHIR mapping")
  logger.info("Action: Map valid condition record to FHIR and validate")
  logger.info("Expected Result: Return value should be 'PASSED'")
  new_mapper_dmdiag2cond = mapper_dmdiag2cond.MapperDMDiag2Cond(logger, systems)
  new_mapper_dmdiag2cond.read(encounter_psn, patient_psn, valid_record, logger)
  try:
    condition = new_mapper_dmdiag2cond.map()
    condition[0].as_json()
    logger.info("Actual Result: PASSED")
  except Exception as exc:
    logger.error("Actual Result: FAILED")
    raise

  logger.info("Step: Negative test condition FHIR mapping")
  logger.info("Action: Map invalid condition record to FHIR and validate")
  logger.info("Expected Result: Return value should be 'FAILED'")
  new_mapper_dmdiag2cond = mapper_dmdiag2cond.MapperDMDiag2Cond(logger, systems)
  new_mapper_dmdiag2cond.read(encounter_psn, patient_psn, invalid_record, logger)
  with pytest.raises(FHIRValidationError):
    condition = new_mapper_dmdiag2cond.map()
    condition[0].as_json()
    logger.error("Actual Result: PASSED")
  logger.info("Actual Result: FAILED")

def _test_procedure_mapper(valid_record, invalid_record, logger, systems, map_table):
  patient_psn = 'dic-pid-110'
  encounter_psn = 'dic-eid-110'
  logger.info("Step: Positive test procedure FHIR mapping")
  logger.info("Action: Map valid procedure record to FHIR and validate")
  logger.info("Expected Result: Return value should be 'PASSED'")
  new_mapper_dmpro2pro_med = mapper_dmpro2pro_med.MapperDMPro2ProMed(logger, systems, map_table)
  new_mapper_dmpro2pro_med.read(encounter_psn, patient_psn,
                                valid_record.procedure_begin_timestamp,
                                valid_record.procedure_end_timestamp, valid_record)
  try:
    [procedure, medication,
     medication_stm] = new_mapper_dmpro2pro_med.map()
    procedure.as_json()
    medication.as_json()
    medication_stm.as_json()
    logger.info("Actual Result: PASSED")
  except Exception as exc:
    logger.error("Actual Result: FAILED")
    raise

  logger.info("Step: Negative test procedure FHIR mapping")
  logger.info("Action: Map invalid procedure record to FHIR and validate")
  logger.info("Expected Result: Return value should be 'FAILED'")
  new_mapper_dmpro2pro_med = mapper_dmpro2pro_med.MapperDMPro2ProMed(logger, systems, map_table)
  new_mapper_dmpro2pro_med.read(encounter_psn, patient_psn,
                                invalid_record.procedure_begin_timestamp,
                                invalid_record.procedure_end_timestamp, invalid_record)
  with pytest.raises(FHIRValidationError):
    [procedure, medication,
     medication_stm] = new_mapper_dmpro2pro_med.map()
    procedure.as_json()
    logger.error("Actual Result: PASSED")
  logger.info("Actual Result: FAILED")

def _test_lab_observation_mapper(valid_record, invalid_record, logger, systems):
  patient_psn = 'dic-pid-110'
  encounter_psn = 'dic-eid-110'
  loinc_url = None
  logger.info("Step: Positive test lab observation FHIR mapping")
  logger.info("Action: Map valid lab observation record to FHIR and validate")
  logger.info("Expected Result: Return value should be 'PASSED'")
  new_mapper_dmlab2obs = mapper_dmlab2obs.MapperDMLab2Obs(logger, systems, loinc_url)
  new_mapper_dmlab2obs.read(encounter_psn, patient_psn, valid_record)
  try:
    lab_obs = new_mapper_dmlab2obs.map()
    lab_obs.as_json()
    logger.info("Actual Result: PASSED")
  except Exception as exc:
    logger.error("Actual Result: FAILED")
    raise

  logger.info("Step: Negative test lab observation FHIR mapping")
  logger.info("Action: Map invalid lab observation record to FHIR and validate")
  logger.info("Expected Result: Return value should be 'FAILED'")
  new_mapper_dmlab2obs = mapper_dmlab2obs.MapperDMLab2Obs(logger, systems, loinc_url)
  new_mapper_dmlab2obs.read(encounter_psn, patient_psn, invalid_record)
  with pytest.raises(FHIRValidationError):
    lab_obs = new_mapper_dmlab2obs.map()
    lab_obs.as_json()
    logger.error("Actual Result: PASSED")
  logger.info("Actual Result: FAILED")

def test(cfile):
  logging.basicConfig(level=logging.INFO,
                      format="%(asctime)s [%(levelname)s] %(message)s",
                      handlers=[logging.FileHandler('debug.log'),
                                logging.StreamHandler()])
  logger = logging.getLogger(__name__)
  config = configparser.ConfigParser()
  config.read(cfile)
  pd.options.mode.chained_assignment = None
  admissionreason_1_2 = pd.read_csv(config['dat_paths']['admissionreason_1_2'])
  admissionreason_3_4 = pd.read_csv(config['dat_paths']['admissionreason_3_4'])
  dischargereason_1_2 = pd.read_csv(config['dat_paths']['dischargereason_1_2'])
  dischargereason_3 = pd.read_csv(config['dat_paths']['dischargereason_3'])
  dep_codes = pd.read_csv(config['dat_paths']['department_codes'])
  map_table_set = []
  map_table_set.append([admissionreason_1_2, admissionreason_3_4,
                       dischargereason_1_2, dischargereason_3])
  ops_drug_mapping = pd.read_csv(config['dat_paths']['ops_drug_mapping'], sep=';')
  drug_unii_mapping = pd.read_csv(config['dat_paths']['drug_unii_mapping'], sep=';')
  map_table_set.append([ops_drug_mapping, drug_unii_mapping])
  map_table_set.append(dep_codes)
  systems = config['systems']
  psn_url = config['server']['url_gpas']

  logger.info("I. Test mapper")
  df = pd.read_csv("/opt/dm_lab2fhir_inc/test/test_db/test_data/rf_med_cov_patient.csv",
                   dtype={'patient_address_zipcode': object})
  valid_record = df.iloc[1]
  invalid_record = df.iloc[0]
  invalid_record['patient_lastname'] = None
  _test_patient_mapper(valid_record, invalid_record, logger, systems)


  df = pd.read_csv("/opt/dm_lab2fhir_inc/test/test_db/test_data/rf_med_cov_encounter.csv",
                   dtype={'admission_event_reason': object, 'discharge_event_reason': object})
  df['admission_timestamp'] = pd.to_datetime(df['admission_timestamp'], utc=True)
  df['discharge_timestamp'] = pd.to_datetime(df['discharge_timestamp'], utc=True)
  valid_record = df.iloc[0]
  invalid_record = df.iloc[0]
  invalid_record['admission_timestamp'] = None
  _test_encounter_mapper(valid_record, invalid_record, logger, systems, map_table_set[0])


  df = pd.read_csv("/opt/dm_lab2fhir_inc/test/test_db/test_data/rf_med_cov_diagnosis.csv")
  df['diagnosis_documentation_timestamp'] = pd.to_datetime(df['diagnosis_documentation_timestamp'],
                                            utc=True)
  valid_record = df.iloc[0]
  invalid_record = df.iloc[0]
  invalid_record['diagnosis_documentation_timestamp'] = None
  _test_condition_mapper(valid_record, invalid_record, logger, systems)


  df = pd.read_csv("/opt/dm_lab2fhir_inc/test/test_db/test_data/rf_med_cov_procedure.csv",
                   dtype={'ops_id': object})
  df['procedure_begin_timestamp'] = pd.to_datetime(df['procedure_begin_timestamp'], utc=True)
  df['procedure_end_timestamp'] = pd.to_datetime(df['procedure_end_timestamp'], utc=True)
  valid_record = df.iloc[16]
  invalid_record = df.iloc[0]
  invalid_record['ops_code'] = None
  _test_procedure_mapper(valid_record, invalid_record, logger, systems, map_table_set[1])


  df = pd.read_csv("/opt/dm_lab2fhir_inc/test/test_db/test_data/f_med_lab_result.csv")
  df['collection_timestamp'] = pd.to_datetime(df['collection_timestamp'], utc=True)
  valid_record = df.iloc[0]
  invalid_record = df.iloc[0]
  invalid_record['collection_timestamp'] = None
  _test_lab_observation_mapper(valid_record, invalid_record, logger, systems)



  logger.info("II. Test pseudonymizer")
  new_pseudonymizer = pseudonymizer.Pseudonymizer(logger, psn_url)
  pid_prefix = 'dic-pid-'
  eid_prefix = 'dic-eid-'
  logger.info("Step: Positive test patient id pseudonymizer")
  logger.info("Action: Pseudonymize patient id and check if it could be done "
              f"successfully with the prefix '{pid_prefix}'")
  logger.info("Expected Result: Return value should be 'PASSED'")
  patient_psn = new_pseudonymizer.request_patient_psn('test_patient_id')
  try:
    assert re.search(rf"({pid_prefix})(.*)", patient_psn)
    logger.info(f"Actual Result: PASSED")
  except AssertionError as exc:
    logger.error(f"Actual Result: FAILED")
    raise


  logger.info("Step: Positive test encounter id pseudonymizer")
  logger.info("Action: Pseudonymize patient id and check if it could be done "
              f"successfully with the prefix '{eid_prefix}'")
  logger.info("Expected Result: Return value should be 'PASSED'")
  encounter_psn = new_pseudonymizer.request_encounter_psn('test_encounter_id')
  try:
    assert re.search(rf"({eid_prefix})(.*)", encounter_psn)
    logger.info(f"Actual Result: PASSED")
  except AssertionError as exc:
    logger.error(f"Actual Result: FAILED")
    raise



  logger.info("III. Test FHIR bundle function")
  new_fhir_bundle = fhir_bundle.FHIRBundle(logger)
  with open('/opt/dm_lab2fhir_inc/test/test_db/test_data/valid_patient', 'r') as file:
    json_obj = json.load(file)
  json_obj['deceasedBoolean'] = True
  new_patient = mii_patient.Patient(json_obj)
  logger.info("Step: Positive test inserting FHIR resource")
  logger.info("Action: Add FHIR resource of type Patient to FHIR bundle, send it to "
              "postgreSQL database and check if it could be done successfully")
  logger.info("Expected Result: Return value should be 'PASSED'")
  new_fhir_bundle.add_resources([new_patient])
  db_con = umm_db_lib.DBConnectionUMM(config, 'fhir_db', logger)
  db_con_fhir = db_con.get_engine()
  dest = new_fhir_bundle.UMMDestination('psql', db_con_fhir)
  new_fhir_bundle.execute(dest)
  nof_ups, nof_rm = umm_db_lib.get_ups_rm_num_fhir('Patient', '2020-12-01', dest.endpoint)
  try:
    assert nof_ups == 1 and nof_rm == 0
    logger.info("Actual Result: PASSED")
  except AssertionError as exc:
    logger.error(f"Actual Result: FAILED")
    raise


  logger.info("Step: Positive test removing FHIR resource")
  logger.info("Action: Remove FHIR resource of type Patient from postgreSQL database and "
              "check if it could be done successfully")
  logger.info("Expected Result: Return value should be 'PASSED'")
  new_fhir_bundle.reset()
  new_fhir_bundle.rm_resources('Patient', 'dic-pid-110')
  new_fhir_bundle.execute(dest)
  nof_ups, nof_rm = umm_db_lib.get_ups_rm_num_fhir('Patient', '2020-12-01', dest.endpoint)
  try:
    assert nof_ups == 0 and nof_rm == 1
    logger.info("Actual Result: PASSED")
  except AssertionError as exc:
    logger.error(f"Actual Result: FAILED")
    raise

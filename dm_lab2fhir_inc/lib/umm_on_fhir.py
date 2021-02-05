#!/usr/bin/python3.6

'''Load sap-ish, lufu, lab data from data warehouse and staging area,
   pseudomyize it and map it to FHIR resources of type Patient, Observation,
   Condition, Procedure, Medication, MedicationStatement, Encounter,
   ObservationReport using an incremental update approach
   Arguments: config, logger
   Returns: none
   Authors: Jan Scheer, jan.scheer@medma.uni-heidelberg.de
            Lukas Goetz, Lukas.Goetz@medma.uni-heidelberg.de
   Date: 01-04-2021'''

import pandas as pd
from datetime import datetime

from lib.mii_profiles import fhirreference
from lib.mii_profiles.fhirabstractbase import FHIRValidationError

from . import umm_db_lib
from . import (mapper_dmpat2pat, mapper_dmlab2obs, mapper_dmdiag2cond,
               mapper_dmenc2obs, mapper_dmpro2pro_med, mapper_dmenc2enc,
               mapper_dmdep2enc, mapper_dmtrans2obs,
               mapper_lufu_snomed_lookup, mapper_lufu_i2b2basecode_lookup,
               mapper_lufufall2obs, mapper_lufufall2rep, mapper_lufu_loinc_lookup,
               mapper_lufufall2proc, mapper_lufu_procedure_lookup,
               fhir_bundle, pseudonymizer)

class UMMPeriod:
  def __init__(self, start, end):
    self.start = start
    self.end = end

class UMMDestination:
  def __init__(self, dtype, endpoint):
    self.dtype = dtype
    self.endpoint = endpoint

class UMMonFHIR:
  '''Load sap-ish, lung function, lab data from DB, pseudomyize it
     and map it to FHIR resources of type Patient, Observation,
     Procedure, Medication, MedicationStatement, Encounter,
     Condition and DiagnosticReport
   Arguments: config, logger'''

  def __init__(self, config, logger):
    admissionreason_1_2 = pd.read_csv(config['dat_paths']['admissionreason_1_2'])
    admissionreason_3_4 = pd.read_csv(config['dat_paths']['admissionreason_3_4'])
    dischargereason_1_2 = pd.read_csv(config['dat_paths']['dischargereason_1_2'])
    dischargereason_3 = pd.read_csv(config['dat_paths']['dischargereason_3'])
    dep_codes = pd.read_csv(config['dat_paths']['department_codes'])
    self.systems = config['systems']

    self.map_table = []
    self.map_table.append([admissionreason_1_2, admissionreason_3_4,
                           dischargereason_1_2, dischargereason_3])
    ops_drug_mapping = pd.read_csv(config['dat_paths']['ops_drug_mapping'], sep=';')
    drug_unii_mapping = pd.read_csv(config['dat_paths']['drug_unii_mapping'], sep=';')
    self.map_table.append([ops_drug_mapping, drug_unii_mapping])
    self.map_table.append(dep_codes)
    lufuloincmapping = pd.read_csv(config['dat_paths']['lufu_loinc_mapping'], encoding='utf-8')
    self.map_table.append(lufuloincmapping)

    self.input_chunk_size = 100
    self.psn_url = config['server']['url_gpas']
    self.loinc_url = config['server']['url_loinc_converter']
    self.logger = logger

  def process_patients(self, period, db_con_dwh, dest, verbose):
      new_pseudonymizer = pseudonymizer.Pseudonymizer(self.logger, self.psn_url)
      new_fhir_bundle = fhir_bundle.FHIRBundle(self.logger)
      new_mapper_dmpat2pat = mapper_dmpat2pat.MapperDMPat2Pat(self.logger,
                                                              self.systems)    
      self.logger.info("I. Process new/ updated/ canceled patient records "
                      f"between {period.start} and {period.end} ...")
      sql_query_pat = f'''WITH ups_pat AS (
                                 SELECT *
                                 FROM dwh.rf_med_cov_patient
                                 WHERE patient_last_update > '{period.start}' AND
                                       patient_last_update < '{period.end}'),
                               del_pat AS (
                                 SELECT stdat, patnr
                                 FROM stg_sap.q_npat
                                 WHERE stdat > '{period.start}' AND stdat < '{period.end}')
                          SELECT * FROM ups_pat
                          FULL OUTER JOIN del_pat
                          ON ups_pat.patient_id = del_pat.patnr::int
                          ORDER BY stdat DESC'''
      first_upsert = True
      first_rm = True
      added_res_pat = 0
      res_pat_invalid = 0
      rm_res_pat = 0
      for chunk in pd.read_sql_query(sql_query_pat, db_con_dwh,
                                     chunksize=self.input_chunk_size):
        for record in chunk.itertuples():
          # Upsert FHIR patient resources
          if not record.stdat:
            if first_upsert:
              self.logger.info("Create & validate FHIR Patient resources ...")
              first_upsert = False

            try:
              patient_psn = new_pseudonymizer.request_patient_psn(record.patient_id)
              new_mapper_dmpat2pat.read(patient_psn, record)
              inpatient = new_mapper_dmpat2pat.map()
              inpatient.as_json()
              new_fhir_bundle.add_resources([inpatient])
              added_res_pat += 1
            except FHIRValidationError:
              res_pat_invalid += 1
              self.logger.debug("Validation error for created Patient resource "
                               f"(id: {patient_psn})")

         # Remove canceled FHIR patient resources
          else:
            if first_rm:
              self.logger.info("Create request to remove canceled FHIR Patient resources ...")
              first_rm = False
            patient_psn = new_pseudonymizer.request_patient_psn(record.patnr)
            new_fhir_bundle.rm_resources('Patient', patient_psn)
            rm_res_pat += 1

      res_stats = {'valid_pat': added_res_pat, 'invalid_pat': res_pat_invalid,
                   'rm_req_pat': rm_res_pat}

      if verbose:
        self.logger.info("Results:")
        self.logger.info(f"Created {res_stats['valid_pat']+res_stats['invalid_pat']} "
                          "Patient resources in total")
        if res_stats['invalid_pat']:
          self.logger.warning(f"Detected {res_stats['invalid_pat']} invalid Patient resources "
                               "being discarded")
        else:
          self.logger.info(f"Detected NO invalid Patient resources")
        self.logger.info(f"Created {res_stats['rm_req_pat']} requests to delete "
                          "Patient resources")

      if dest.dtype == 'psql':
        current_ts = datetime.now()
        print(current_ts)
        new_fhir_bundle.execute(dest)
        ups_res_pat, rm_res_pat = umm_db_lib.get_ups_rm_num_fhir('Patient', current_ts,
                                                                 dest.endpoint)
        res_stats_ext = {'ups_pat': ups_res_pat, 'rm_pat': rm_res_pat}
        res_stats.update(res_stats_ext)
        if verbose:
          self.logger.info("Results:")
          self.logger.info(f"Upserted {res_stats['ups_pat']} Patient resources")
          self.logger.info(f"Removed {res_stats['rm_pat']} Patient resources")
      else:
        new_fhir_bundle.execute(dest)      

      return res_stats

  def process_encounters(self, period, db_con_dwh, dest, verbose):
      new_pseudonymizer = pseudonymizer.Pseudonymizer(self.logger, self.psn_url)
      new_fhir_bundle = fhir_bundle.FHIRBundle(self.logger)
      new_mapper_dmdiag2cond = mapper_dmdiag2cond.MapperDMDiag2Cond(self.logger,
                                                                    self.systems)
      new_mapper_dmenc2obs = mapper_dmenc2obs.MapperDMEnc2Obs(self.logger, self.systems)
      new_mapper_dmenc2enc = mapper_dmenc2enc.MapperDMEnc2Enc(self.logger, self.systems,
                                                              self.map_table[0])
      new_mapper_dmdep2enc = mapper_dmdep2enc.MapperDMDep2Enc(self.logger, self.systems,
                                                              self.map_table[2])
      self.logger.info("II. Process new/ updated/ canceled encounter records "
                      f"between {period.start} and {period.end} ...")
      sql_query_enc = f'''WITH ups_enc AS (
                                 SELECT encounter_id, patient_id,
                                        admission_event_reason, discharge_event_reason,
                                        admission_timestamp, discharge_timestamp,
                                        ventilation_hours, encounter_last_update
                                 FROM dwh.rf_med_cov_encounter
                                 WHERE encounter_last_update > '{period.start}' AND
                                       encounter_last_update < '{period.end}'),
                               del_enc AS (
                                 SELECT stdat, falnr
                                 FROM stg_sap.q_nfal
                                 WHERE stdat > '{period.start}' AND stdat < '{period.end}')
                          SELECT * FROM ups_enc
                          FULL OUTER JOIN del_enc
                          ON ups_enc.encounter_id = del_enc.falnr::int
                          ORDER BY stdat DESC'''
      first_upsert = True
      first_rm = True
      added_res_subenc = 0
      res_subenc_invalid = 0
      added_res_enc = 0
      res_enc_invalid = 0
      added_res_obs = 0
      res_obs_invalid = 0
      added_res_con = 0
      res_con_invalid = 0
      added_res_loc = 0
      res_loc_invalid = 0
      rm_res_enc = 0
      for chunk in pd.read_sql_query(sql_query_enc, db_con_dwh,
                                     chunksize=self.input_chunk_size):
        for record in chunk.itertuples():
          # Upsert new/ updated encounters
          if not record.stdat:
            if first_upsert:
              self.logger.info("Create & validate FHIR Encounter, Condition, "
                               "Observation (ventilation) resources ...")
              first_upsert = False
            # Extract and map conditions
            condition_list = []
            cond_chunk = pd.read_sql_query(f'''SELECT * FROM dwh.rf_med_cov_diagnosis
                                               WHERE encounter_id =
                                               {record.encounter_id}''', db_con_dwh)
            patient_psn = new_pseudonymizer.request_patient_psn(record.patient_id)
            encounter_psn = new_pseudonymizer.request_encounter_psn(record.encounter_id)

            for cond_record in cond_chunk.itertuples():
              try:
                new_mapper_dmdiag2cond.read(encounter_psn, patient_psn, cond_record,
                                            self.logger)
                icd_condition = new_mapper_dmdiag2cond.map()
                icd_condition[0].as_json()
                condition_list.append(icd_condition)
                added_res_con += 1
              except FHIRValidationError:
                self.logger.debug("Validation error for created Condition resource "
                                 f"(id: {encounter_psn}-"
                                 f"{cond_record.diagnosis_nr})")
                res_con_invalid += 1

            ranked_cond_ref = []
            condition_list_2 = []
            for condition_rank_tuple in condition_list:
              condition = condition_rank_tuple[0]
              condition_rank = condition_rank_tuple[1]
              ref = {'reference': f"Condition/{condition.id}"}
              condition_ref = fhirreference.FHIRReference(jsondict=ref)
              ranked_cond_ref.append([condition_ref, condition_rank])
              condition_list_2.append(condition)

            sub_encounter_list = []
            location_list_total = []
            # Extract p301 departments
            department_set = pd.read_sql_query(f'''SELECT DISTINCT dwh_unit.dept_p301_code
                                                   FROM dwh.rf_med_cov_transfer dwh_trans
                                                   JOIN dwh.rd_med_cov_unit dwh_unit
                                                   ON dwh_unit.unit_id = dwh_trans.event_unit_id
                                                   WHERE encounter_id = {record.encounter_id}''',
                                                   db_con_dwh)
            ## Extract and map transfers within each p301 department
            for department in department_set.itertuples():
            #  # todo: adjust timezone, currently UTC?!
              transfer_set = pd.read_sql_query(f'''SELECT *
                                                   FROM dwh.rf_med_cov_transfer dwh_trans
                                                   JOIN dwh.rd_med_cov_unit dwh_unit
                                                   ON dwh_unit.unit_id = dwh_trans.event_unit_id
                                                   WHERE encounter_id = {record.encounter_id} AND
                                                         dwh_unit.dept_p301_code =
                                                  '{department.dept_p301_code}' ''', db_con_dwh)
              new_mapper_dmdep2enc.read(patient_psn, encounter_psn, department.dept_p301_code,
                                        transfer_set)
              (sub_encounter, location_list,
               added_res_loc2, res_loc_invalid2) = new_mapper_dmdep2enc.map()
              added_res_loc += added_res_loc2
              res_loc_invalid += res_loc_invalid2
              location_list_total = location_list_total + location_list
              try:
                sub_encounter.as_json()
                sub_encounter_list.append(sub_encounter)
                added_res_subenc += 1
              except FHIRValidationError:
                self.logger.debug("Validation error for created Subencounter resource "
                                 f"(id: {encounter_psn})")
                res_subenc_invalid += 1
                added_res_loc -= added_res_loc2
                res_loc_invalid -= res_loc_invalid2

            try:
              new_mapper_dmenc2obs.read(encounter_psn, patient_psn, record)
              vent_observation = new_mapper_dmenc2obs.map()
              vent_observation.as_json()
              new_fhir_bundle.add_resources([vent_observation])
              added_res_obs += 1
            except FHIRValidationError:
              self.logger.debug("Validation error for created Observation resource "
                               f"(id: {encounter_psn}-vent)")
              res_obs_invalid += 1

            try:
              new_mapper_dmenc2enc.read(ranked_cond_ref, patient_psn,
                                        encounter_psn, record)
              main_encounter = new_mapper_dmenc2enc.map()
              main_encounter.as_json()
              new_fhir_bundle.add_resources([main_encounter])
              new_fhir_bundle.add_resources(condition_list_2)
              new_fhir_bundle.add_resources(sub_encounter_list)
              new_fhir_bundle.add_resources(location_list_total)              
              added_res_enc += 1
            except FHIRValidationError:
              self.logger.debug("Validation error for created Encounter resource "
                               f"(id: {encounter_psn})")
              res_enc_invalid += 1
              # remove conditions/ subencounters/locations of invalid encounters
              added_res_con -= len(condition_list_2)
              added_res_subenc -= len(sub_encounter_list)
              added_res_loc -= len(location_list_total)
              # todo adjust invalid counter as well

          else:
            if first_rm:
              self.logger.info("Create request to remove canceled FHIR Encounter, "
                               "Observation (ventilation) resources ...")
              first_rm = False
            encounter_psn = new_pseudonymizer.request_encounter_psn(record.falnr)
            new_fhir_bundle.rm_resources('Encounter', encounter_psn)
            obs_id = encounter_psn + '_vent'
            new_fhir_bundle.rm_resources('Observation', obs_id)
            rm_res_enc += 1

      res_stats = {'valid_con': added_res_con, 'invalid_con': res_con_invalid,
                   'valid_enc': added_res_enc, 'invalid_enc': res_enc_invalid,
                   'valid_subenc': added_res_subenc, 'invalid_subenc': res_subenc_invalid,
                   'valid_loc': added_res_loc, 'invalid_loc': res_loc_invalid,
                   'valid_obs': added_res_obs, 'invalid_obs': res_obs_invalid,
                   'rm_req_enc': rm_res_enc}

      if verbose:
        self.logger.info("Results:")
        self.logger.info(f"Created {res_stats['valid_con']+res_stats['invalid_con']} "
                      "Condition resources in total")
        self.logger.info(f"Created {res_stats['valid_enc']+res_stats['invalid_enc']} "
                      "Encounter resources in total")
        self.logger.info(f"Created {res_stats['valid_subenc']+res_stats['invalid_subenc']} "
                      "Subencounter resources in total")
        self.logger.info(f"Created {res_stats['valid_loc']+res_stats['invalid_loc']} "
                      "Location resources in total")
        self.logger.info(f"Created {res_stats['valid_obs']+res_stats['invalid_obs']} "
                      "Observation (ventilation) resources in total")
        if (res_stats['invalid_con'] or res_stats['invalid_enc'] or
            res_stats['invalid_subenc'] or res_stats['invalid_loc'] or
            res_stats['invalid_obs']):
          self.logger.warning(f"Detected {res_stats['invalid_con']} invalid Condition/ "
                          f"{res_stats['invalid_enc']} invalid Encounter/ "
                          f"{res_stats['invalid_subenc']} invalid Subencounter/ "
                          f"{res_stats['invalid_loc']} invalid Location/ "
                          f"{res_stats['invalid_obs']} invalid Observation (ventilation) "
                           "resources being discarded")
        else:
          self.logger.info(f"Detected NO invalid Condition/ Encounter/ Subencounter/ Location "
                        "Observation (ventilation) resources")
        self.logger.info(f"Created {res_stats['rm_req_enc']} requests to delete Encounter/ Observation "
                      "(ventilation) resources")    
      if dest.dtype == 'psql':
        current_ts = datetime.now()
        new_fhir_bundle.execute(dest)
        ups_res_con, rm_res_con = umm_db_lib.get_ups_rm_num_fhir('Condition', current_ts,
                                                                   dest.endpoint)
        ups_res_enc, rm_res_enc = umm_db_lib.get_ups_rm_num_fhir('Encounter', current_ts,
                                                                   dest.endpoint)
        ups_res_obs, rm_res_obs = umm_db_lib.get_ups_rm_num_fhir('Obs_vent', current_ts,
                                                                   dest.endpoint)
        res_stats_ext = {'ups_con': ups_res_con, 'rm_con': rm_res_con,
                         'ups_enc': ups_res_enc, 'rm_enc': rm_res_enc,
                         'ups_obs': ups_res_obs, 'rm_obs': rm_res_obs}
        res_stats.update(res_stats_ext)
        if verbose:
          self.logger.info("Results:")
          self.logger.info(f"Upserted {res_stats['ups_con']} Condition resources")
          self.logger.info(f"Upserted {res_stats['ups_enc']} Encounter resources")
          self.logger.info(f"Removed {res_stats['rm_enc']} Encounter resources")
          self.logger.info(f"Upserted {res_stats['ups_obs']} Observation (ventilation) resources")
          self.logger.info(f"Removed {res_stats['rm_obs']} Observation (ventilation) resources")
      else:
        new_fhir_bundle.execute(dest)

      return res_stats
  
  def process_transfers(self, period, db_con_dwh, dest, verbose):
      new_pseudonymizer = pseudonymizer.Pseudonymizer(self.logger, self.psn_url)    
      new_fhir_bundle = fhir_bundle.FHIRBundle(self.logger)
      new_mapper_dmtrans2obs = mapper_dmtrans2obs.MapperDMTrans2Obs(self.logger, self.systems)
      self.logger.info("III. Process new/ updated/ canceled transfer records "
                      f"between {period.start} and {period.end} ...")
      sql_query_trans = f'''WITH nbew_extr AS (
                                  SELECT falnr AS falnr_upsert, lfdnr AS lfdnr_upsert,
                                         updat, erdat, encounter_id AS encounter_id_enc,
                                         icu_days, intercurrent_dialyses,
                                         admission_timestamp, discharge_timestamp
                                  FROM dwh.rf_med_cov_encounter, stg_sap.q_nbew
                                  WHERE GREATEST(updat, erdat) > '{period.start}' AND
                                        GREATEST(updat, erdat) < '{period.end}' AND
                                        falnr::int = encounter_id),
                                ups_trans AS (
                                  SELECT *
                                  FROM dwh.rf_med_cov_transfer, nbew_extr
                                  WHERE encounter_id = falnr_upsert::int AND
                                        event_nr = lfdnr_upsert::int),
                                del_trans AS (
                                  SELECT stdat, falnr AS falnr_delete,
                                         lfdnr AS lfdnr_delete
                                  FROM stg_sap.q_nbew
                                  WHERE stdat > '{period.start}' AND stdat < '{period.end}')
                           SELECT DISTINCT encounter_id, patient_id, icu_days,
                                  intercurrent_dialyses, admission_timestamp,
                                  discharge_timestamp, falnr_delete, stdat
                           FROM ups_trans
                           FULL OUTER JOIN del_trans
                           ON ups_trans.encounter_id = del_trans.falnr_delete::int AND
                              ups_trans.event_nr = del_trans.lfdnr_delete::int
                           ORDER BY stdat DESC'''
      first_upsert = True
      first_rm = True
      added_res_dial_obs = 0
      res_dial_obs_invalid = 0
      added_res_icu_obs = 0
      res_icu_obs_invalid = 0
      rm_res = 0
      for chunk in pd.read_sql_query(sql_query_trans, db_con_dwh,
                                     chunksize=self.input_chunk_size):
        for record in chunk.itertuples():
          # Upsert new/ updated transfers
          if not record.stdat:
            if first_upsert:
              self.logger.info("Create & validate FHIR Observation (ICU days, dialysis) "
                               "resources ...")
              first_upsert = False
            patient_psn = new_pseudonymizer.request_patient_psn(record.patient_id)
            encounter_psn = new_pseudonymizer.request_encounter_psn(record.encounter_id)


            new_mapper_dmtrans2obs.read(encounter_psn, patient_psn, record)
            [dialysis_obs, icu_obs] = new_mapper_dmtrans2obs.map()
            try:
              dialysis_obs.as_json()
              new_fhir_bundle.add_resources([dialysis_obs])
              added_res_dial_obs += 1
            except FHIRValidationError as exc:
              self.logger.debug("Validation error for created Observation resource "
                               f"(id: {encounter_psn}-dia)")
              res_dial_obs_invalid += 1
            try:
              icu_obs.as_json()
              new_fhir_bundle.add_resources([icu_obs])
              added_res_icu_obs += 1
            except FHIRValidationError:
              self.logger.debug("Validation error for created Observation resource "
                               f"(id: {encounter_psn}-icu)")
              res_icu_obs_invalid += 1
          else:
            if first_rm:
              self.logger.info("Create request to remove canceled FHIR Observation "
                               "(ICU days, dialysis) resources ...")
              first_rm = False
            encounter_psn = new_pseudonymizer.request_encounter_psn(record.falnr_delete)
            obs_id = encounter_psn + '_icu'
            new_fhir_bundle.rm_resources('Observation', obs_id)
            obs_id = encounter_psn + '_dia'
            new_fhir_bundle.rm_resources('Observation', obs_id)
            rm_res += 1

      res_stats = {'valid_dial': added_res_dial_obs, 'invalid_dial': res_dial_obs_invalid,
                   'valid_icu': added_res_icu_obs, 'invalid_icu': res_icu_obs_invalid,
                   'rm_req_res': rm_res}

      if verbose:
        self.logger.info("Results:")
        self.logger.info(f"Created {res_stats['valid_dial']+res_stats['invalid_dial']} "
                      "Observation (dialysis) resources in total")
        self.logger.info(f"Created {res_stats['valid_icu']+res_stats['invalid_icu']} "
                      "Observation (ICU days) resources in total")
        if res_stats['invalid_dial'] or res_stats['invalid_icu']:
          self.logger.warning(f"Detected {res_stats['invalid_dial']} invalid Observation "
                          f"(dialysis)/ {res_stats['invalid_icu']} invalid Observation "
                          f"(ICU days) resources being discarded")
        else:
          self.logger.info(f"Detected NO invalid Observation (dialysis, ICU days) resources")
        self.logger.info(f"Created {res_stats['rm_req_res']} requests to delete Observation "
                      "(dialysis, ICU days) resources")
      if dest.dtype == 'psql':
        current_ts = datetime.now()
        new_fhir_bundle.execute(dest)
        ups_res_dial, rm_res_dial = umm_db_lib.get_ups_rm_num_fhir('Obs_dial', current_ts,
                                                                     dest.endpoint)
        ups_res_icu, rm_res_icu = umm_db_lib.get_ups_rm_num_fhir('Obs_icu', current_ts,
                                                                    dest.endpoint)
        res_stats_ext = {'ups_dial': ups_res_dial, 'rm_dial': rm_res_dial,
                         'ups_icu': ups_res_icu, 'rm_icu': rm_res_icu}
        res_stats.update(res_stats_ext)
        if verbose:
          self.logger.info("Results:")
          self.logger.info(f"Upserted {res_stats['ups_dial']} Observation (dialysis) resources")
          self.logger.info(f"Removed {res_stats['rm_dial']} Observation (dialysis) resources")
          self.logger.info(f"Upserted {res_stats['ups_icu']} Observation (ICU days) resources")
          self.logger.info(f"Removed {res_stats['rm_icu']} Observation (ICU days) resources")   
      else:
        new_fhir_bundle.execute(dest)

      return res_stats

  def process_conditions(self, period, db_con_dwh, dest, verbose):
      new_pseudonymizer = pseudonymizer.Pseudonymizer(self.logger, self.psn_url)      
      new_fhir_bundle = fhir_bundle.FHIRBundle(self.logger)
      new_mapper_dmdiag2cond = mapper_dmdiag2cond.MapperDMDiag2Cond(self.logger,
                                                                    self.systems)    
      self.logger.info("IV. Process new/ updated/ canceled diagnosis records "
                      f"between {period.start} and {period.end} ...")
      sql_query_cond = f'''WITH ndia_extr AS (
                                  SELECT falnr AS falnr_upsert, lfdnr AS lfdnr_upsert,
                                         updat, erdat
                                  FROM stg_sap.q_ndia),
                                ups_cond AS (
                                  SELECT *
                                  FROM dwh.rf_med_cov_diagnosis, ndia_extr
                                  WHERE encounter_id = falnr_upsert::int AND
                                        diagnosis_nr = lfdnr_upsert::int AND
                                        GREATEST(updat, erdat) > '{period.start}' AND
                                        GREATEST(updat, erdat) < '{period.end}'),
                                del_cond AS (
                                  SELECT stdat, falnr AS falnr_delete,
                                         lfdnr AS lfdnr_delete
                                  FROM stg_sap.q_ndia
                                  WHERE stdat > '{period.start}' AND stdat < '{period.end}')
                           SELECT * FROM ups_cond
                           FULL OUTER JOIN del_cond
                           ON ups_cond.encounter_id = del_cond.falnr_delete::int AND
                              ups_cond.diagnosis_nr = del_cond.lfdnr_delete::int
                           ORDER BY stdat DESC'''
      first_upsert = True
      first_rm = True
      added_res = 0
      invalid_res = 0
      rm_res = 0
      for chunk in pd.read_sql_query(sql_query_cond, db_con_dwh,
                                     chunksize=self.input_chunk_size):
        for record in chunk.itertuples():
          if not record.stdat:
            if first_upsert:
              self.logger.info("Create & validate FHIR Condition resources ...")
              first_upsert = False
            patient_psn = new_pseudonymizer.request_patient_psn(record.patient_id)
            encounter_psn = new_pseudonymizer.request_encounter_psn(record.encounter_id)
            try:
              new_mapper_dmdiag2cond.read(encounter_psn, patient_psn, record, self.logger)
              icd_condition = new_mapper_dmdiag2cond.map()
              icd_condition = icd_condition[0]
              icd_condition.as_json()
              new_fhir_bundle.add_resources([icd_condition])
              added_res += 1
            except FHIRValidationError:
              self.logger.debug("Validation error for created Observation resource "
                               f"(id: {encounter_psn}_{record.diagnosis_nr})")
              invalid_res += 1
          else:
            if first_rm:
              self.logger.info("Create request to remove canceled FHIR Condition resources ...")
              first_rm = False
            encounter_psn = new_pseudonymizer.request_encounter_psn(record.falnr_delete)
            cond_id = encounter_psn + '_' + str(record.lfdnr_delete)
            new_fhir_bundle.rm_resources('Condition', cond_id)
            rm_res += 1

      res_stats = {'valid_con': added_res, 'invalid_con': invalid_res, 'rm_req_con': rm_res}
      if verbose:
        self.logger.info("Results:")
        self.logger.info(f"Created {res_stats['valid_con']+res_stats['invalid_con']} Condition "
                      "resources in total")
        if res_stats['invalid_con']:
          self.logger.warning(f"Detected {res_stats['invalid_con']} invalid Condition resources "
                           "being discarded")
        else:
          self.logger.info(f"Detected NO invalid Condition resources")
        self.logger.info(f"Created {res_stats['rm_req_con']} requests to delete Condition "
                          "resources")

      if dest.dtype == 'psql':
        current_ts = datetime.now()
        new_fhir_bundle.execute(dest)
        ups_res_con, rm_res_con = umm_db_lib.get_ups_rm_num_fhir('Condition', current_ts,
                                                                   dest.endpoint)
        res_stats_ext = {'ups_con': ups_res_con, 'rm_con': rm_res_con}
        res_stats.update(res_stats_ext)
        if verbose:
          self.logger.info("Results:")
          self.logger.info(f"Upserted {res_stats['ups_con']} Condition resources")
          self.logger.info(f"Removed {res_stats['rm_con']} Condition resources")
      else:
        new_fhir_bundle.execute(dest)

      return res_stats

  def process_procedures(self, period, db_con_dwh, dest, verbose):
      new_pseudonymizer = pseudonymizer.Pseudonymizer(self.logger, self.psn_url)      
      new_fhir_bundle = fhir_bundle.FHIRBundle(self.logger)
      new_mapper_dmpro2pro_med = mapper_dmpro2pro_med.MapperDMPro2ProMed(self.logger,
                                                                         self.systems,
                                                                         self.map_table[1])
      self.logger.info("V. Process new/ updated/ canceled procedure records "
                      f"between {period.start} and {period.end} ...")
      sql_query_prod = f'''WITH nicp_extr AS (
                                  SELECT falnr AS falnr_upsert, lnric AS lnric_upsert, updat
                                  FROM stg_sap.q_nicp),
                                ups_prod AS (
                                  SELECT encounter_id, patient_id, procedure_nr, ops_code, ops_id,
                                         procedure_laterality, procedure_begin_timestamp,
                                         procedure_end_timestamp
                                  FROM dwh.rf_med_cov_procedure, nicp_extr
                                  WHERE encounter_id = falnr_upsert::int AND
                                        procedure_nr = lnric_upsert::int AND
                                        updat > '{period.start}' AND updat < '{period.end}'),
                                del_prod AS (
                                  SELECT stdat, falnr AS falnr_delete,
                                         lnric AS lnric_delete,
                                         icpml AS ops_code_delete
                                  FROM stg_sap.q_nicp
                                  WHERE stdat > '{period.start}' AND stdat < '{period.end}')
                           SELECT * FROM ups_prod
                           FULL OUTER JOIN del_prod
                           ON ups_prod.encounter_id = del_prod.falnr_delete::int AND
                              ups_prod.procedure_nr = del_prod.lnric_delete::int
                           ORDER BY stdat DESC'''
      first_upsert = True
      first_rm = True
      added_res_prod = 0
      res_prod_invalid = 0
      added_res_med = 0
      res_med_invalid = 0
      added_res_medstm = 0
      res_medstm_invalid = 0
      rm_res_prod = 0
      for chunk in pd.read_sql_query(sql_query_prod, db_con_dwh,
                                     chunksize=self.input_chunk_size):
        for record in chunk.itertuples():
          if not record.stdat:
            if first_upsert:
              self.logger.info("Create & validate FHIR Procedure/ Medication/ "
                               "MedicationStatement resources ...")
              first_upsert = False
            patient_psn = new_pseudonymizer.request_patient_psn(record.patient_id)
            encounter_psn = new_pseudonymizer.request_encounter_psn(record.encounter_id)
            
            new_mapper_dmpro2pro_med.read(encounter_psn, patient_psn,
                                          record.procedure_begin_timestamp,
                                          record.procedure_end_timestamp, record)

            [procedure, medication,
             medication_stm] = new_mapper_dmpro2pro_med.map()
            try:
              procedure.as_json()
              new_fhir_bundle.add_resources([procedure])
              added_res_prod += 1
            except FHIRValidationError:
              self.logger.debug("Validation error for created Procedure resource "
                               f"(id: {encounter_psn}_{record.procedure_nr})")
              res_prod_invalid += 1
            try:
              if medication:
                medication.as_json()
                new_fhir_bundle.add_resources([medication])
                added_res_med += 1
            except FHIRValidationError:
              self.logger.debug("Validation error for created Medication resource "
                               f"(id: {encounter_psn}_med)")
              res_med_invalid += 1
            try:
              if medication_stm:
                medication_stm.as_json()
                new_fhir_bundle.add_resources([medication_stm])
                added_res_medstm += 1
            except FHIRValidationError:
              self.logger.debug("Validation error for created MedicationStatement resource "
                               f"(id: {encounter_psn}_{record.procedure_nr}_med_stm)")
              res_medstm_invalid += 1
          else:
            if first_rm:
              self.logger.info("Create request to remove canceled FHIR Procedure/ Medication/ "
                               "MedicationStatement resources ...")
              first_rm = False
            encounter_psn = new_pseudonymizer.request_encounter_psn(record.falnr_delete)
            if record.ops_code_delete and record.ops_code_delete[0] == '6':
              med_id = encounter_psn + '_' + str(record.lnric_delete) + '_med_stat'
              new_fhir_bundle.rm_resources('MedicationStatement', med_id)
            else:
              prod_id = encounter_psn + '_' + str(record.lnric_delete)
              new_fhir_bundle.rm_resources('Procedure', prod_id)
            rm_res_prod += 1

      res_stats = {'valid_prod': added_res_prod, 'invalid_prod': res_prod_invalid,
                   'valid_med': added_res_med, 'invalid_med': res_med_invalid,
                   'valid_medstm': added_res_medstm, 'invalid_medstm': res_medstm_invalid,
                   'rm_req_prod': rm_res_prod} 

      if verbose:
        self.logger.info("Results:")
        self.logger.info(f"Created {res_stats['valid_prod']+res_stats['invalid_prod']} "
                      "Procedure resources in total")
        self.logger.info(f"Created {res_stats['valid_med']+res_stats['invalid_med']} "
                      "Medication resources in total")
        self.logger.info(f"Created {res_stats['valid_medstm']+res_stats['invalid_medstm']} "
                      "MedicationStatement resources in total")
        if res_stats['invalid_prod'] or res_stats['invalid_med'] or res_stats['invalid_medstm']:
          self.logger.warning(f"Detected {res_stats['invalid_prod']} invalid Procedure/ "
                          f"{res_stats['invalid_med']} invalid Medication/ "
                          f"{res_stats['invalid_medstm']} invalid MedicationStatement "
                           "resources being discarded")
        else:
          self.logger.info(f"Detected NO invalid Procedure/ Medication/ MedicationStatement "
                        "resources")
        self.logger.info(f"Created {res_stats['rm_req_prod']} requests to delete Procedure/ "
                      "Medication/ MedicationStatement resources")    
      if dest.dtype == 'psql':
        current_ts = datetime.now()
        new_fhir_bundle.execute(dest)
        ups_res_prod, rm_res_prod = umm_db_lib.get_ups_rm_num_fhir('Procedure',
                                                        current_ts, dest.endpoint)
        ups_res_med, rm_res_med = umm_db_lib.get_ups_rm_num_fhir('Medication',
                                                       current_ts, dest.endpoint)
        ups_res_medstm, rm_res_medstm = umm_db_lib.get_ups_rm_num_fhir('MedicationStatement',
                                                            current_ts, dest.endpoint)
        res_stats_ext = {'ups_prod': ups_res_prod, 'rm_prod': rm_res_prod,
                         'ups_med': ups_res_med, 'rm_med': rm_res_med,
                         'ups_medstm': ups_res_medstm, 'rm_medstm': rm_res_medstm}
        res_stats.update(res_stats_ext)
        if verbose:
          self.logger.info("Results:")
          self.logger.info(f"Upserted {res_stats['ups_prod']} Procedure resources")
          self.logger.info(f"Removed {res_stats['rm_prod']} Procedure resources")
          self.logger.info(f"Upserted {res_stats['ups_med']} Medication resources")
          self.logger.info(f"Removed {res_stats['rm_med']} Medication resources")
          self.logger.info(f"Upserted {res_stats['ups_medstm']} MedicationStatement resources")
          self.logger.info(f"Removed {res_stats['rm_medstm']} MedicationStatement resources")
      else:
        new_fhir_bundle.execute(dest)

      return res_stats

  def process_lufu(self, period, db_con_dwh, dest, verbose):
    new_pseudonymizer = pseudonymizer.Pseudonymizer(self.logger, self.psn_url)    
    new_fhir_bundle = fhir_bundle.FHIRBundle(self.logger)
    new_mapper_lufu_loinc = mapper_lufu_loinc_lookup.MapperLuFu2Loinc(self.logger,
                                                                      self.systems)
    new_mapper_lufu_snomed = mapper_lufu_snomed_lookup.MapperLuFu2Snomed(self.logger,
                                                                         self.systems)
    new_mapper_lufu_i2b2 = mapper_lufu_i2b2basecode_lookup.MapperLuFu2i2b2(self.logger,
                                                                           self.systems)
    new_mapper_lufufall2rep = mapper_lufufall2rep.MapperLuFuFall2Rep(self.logger, self.systems)
    new_mapper_lufufall2obs = mapper_lufufall2obs.MapperLuFuFall2Obs(self.logger, self.systems,
                                                      self.map_table[3], new_mapper_lufu_loinc,
                                                  new_mapper_lufu_snomed, new_mapper_lufu_i2b2)
    self.logger.info("VI. Process new/ updated lung function (lufu) records "
                    f"between {period.start} and {period.end} ...")
    sql_query_lufu = f'''WITH dwh_encounter AS (
                            SELECT patient_id, encounter_id AS dwh_encounter_id
                            FROM dwh.rf_med_cov_encounter)
                         SELECT * FROM dwh.f_med_din_lungenfunktion, dwh_encounter
                         WHERE encounter_id::int = dwh_encounter_id AND
                               (sendedatum > '{period.start}' AND sendedatum < '{period.end}') AND
                               untersuchung_status = 'geschlossen'
                         ORDER BY untersuchung_id'''
    added_res_rep = 0
    added_res_obs = 0
    res_rep_invalid = 0
    res_obs_invalid = 0
    self.logger.info("Create & validate FHIR Observation (lufu)/ "
                     "DiagnosticReport resources ...")
    for chunk in pd.read_sql_query(sql_query_lufu, db_con_dwh,
                                   chunksize=self.input_chunk_size):
      for idx, record in chunk.iterrows():
      #for record in chunk.itertuples():
        # create diagnostic report
        patient_psn = new_pseudonymizer.request_patient_psn(record['patient_id'])
        encounter_psn = new_pseudonymizer.request_encounter_psn(record['encounter_id'])
        new_mapper_lufufall2rep.read(encounter_psn, patient_psn, record)
        lufu_diagnostic_report = new_mapper_lufufall2rep.map()
        # create lufu observations and return lists with observations for distinct procedures
        new_mapper_lufufall2obs.read(encounter_psn, patient_psn, record)
        lufu_observation_list = new_mapper_lufufall2obs.map()

        try:
          lufu_diagnostic_report.as_json()

          if lufu_observation_list:

            lufu_obs_ref_list = []

            for obs in lufu_observation_list:
              obs_ref = {"reference": f"Observation/{obs.id}"}
              try:
                obs.as_json()
                lufu_obs_ref_list.append(fhirreference.FHIRReference(jsondict=obs_ref))
                added_res_obs += 1
              except FHIRValidationError:
                res_obs_invalid += 1
                self.logger.debug("Validation error for created Observation (lufu) resource "
                                 f"(id: ??)")

            lufu_diagnostic_report.result = lufu_obs_ref_list
            new_fhir_bundle.add_resources(lufu_observation_list)
            new_fhir_bundle.add_resources([lufu_diagnostic_report])
            added_res_rep += 1
        except FHIRValidationError:
          res_rep_invalid += 1
          self.logger.debug("Validation error for created DiagnosticReport (lufu) resource "
                           f"(id: ??)")
    res_stats = {'valid_obs': len(lufu_obs_ref_list), 'invalid_obs': res_obs_invalid,
                 'valid_rep': added_res_rep, 'invalid_rep': res_rep_invalid}

    if verbose:
      self.logger.info("Results:")
      self.logger.info(f"Created {res_stats['valid_obs']+res_stats['invalid_obs']} "
                        "Observation (lufu) resources in total")
      if res_stats['invalid_obs']:
        self.logger.warning(f"Detected {res_stats['invalid_obs']} invalid Observation (lufu) "
                           "resources being discarded")
      else:
        self.logger.info(f"Detected NO invalid Observation (lufu) resources")

      self.logger.info(f"Created {res_stats['valid_rep']+res_stats['invalid_rep']} "
                      "DiagnosticReport (lufu) resources in total")
      if res_stats['invalid_rep']:
        self.logger.warning(f"Detected {res_stats['invalid_rep']} invalid DiagnosticReport (lufu) "
                           "resources being discarded")
      else:
        self.logger.info(f"Detected NO invalid DiagnosticReport (lufu) resources")    
    if dest.dtype == 'psql':
      current_ts = datetime.now()
      new_fhir_bundle.execute(dest)
      ups_res_obs, rm_res_obs = umm_db_lib.get_ups_rm_num_fhir('Obs_lufu', current_ts,
                                                      dest.endpoint)
      ups_res_rep, rm_res_rep = umm_db_lib.get_ups_rm_num_fhir('DiagnosticReport', current_ts,
                                                      dest.endpoint)
      res_stats_ext = {'ups_obs': ups_res_obs, 'rm_obs': rm_res_obs,
                       'ups_rep': ups_res_rep, 'rm_rep': rm_res_rep}
      res_stats.update(res_stats_ext)
      if verbose:
        self.logger.info("Results:")
        self.logger.info(f"Upserted {res_stats['ups_obs']} Observation (lufu) resources")
        self.logger.info(f"Upserted {res_stats['ups_rep']} DiagnosticReport (lufu) resources")
    else:
      new_fhir_bundle.execute(dest)

    return res_stats

  def process_lab_results(self, period, db_con_dwh, dest, verbose):
    new_pseudonymizer = pseudonymizer.Pseudonymizer(self.logger, self.psn_url)    
    new_fhir_bundle = fhir_bundle.FHIRBundle(self.logger)   
    new_mapper_dmlab2obs = mapper_dmlab2obs.MapperDMLab2Obs(self.logger, self.systems,
                                                            self.loinc_url)
    self.logger.info("VII. Process new/ updated lab records "
                    f"between {period.start} and {period.end} ...")
    sql_query = f'''SELECT * FROM dwh.f_med_lab_result
                    WHERE (collection_timestamp > '{period.start}' AND
                           collection_timestamp < '{period.end}') AND
                           loinc_code <> 'noLoinc'
                    ORDER BY encounter_id '''
    lab_observation_list = []
    res_invalid = 0
    self.logger.info("Create & validate FHIR Observation (laboratory) resources ...")
    for chunk in pd.read_sql_query(sql_query, db_con_dwh,
                                   chunksize=1000):
      for record in chunk.itertuples():      
        patient_psn = new_pseudonymizer.request_patient_psn(record.patient_id)
        encounter_psn = new_pseudonymizer.request_encounter_psn(record.encounter_id)
        new_mapper_dmlab2obs.read(encounter_psn, patient_psn, record)
        lab_observation = new_mapper_dmlab2obs.map()
        try:
          lab_observation.as_json()
          lab_observation_list.append(lab_observation)
        except FHIRValidationError:
          res_invalid += 1

    new_fhir_bundle.add_resources(lab_observation_list)
    res_stats = {'valid_obs': len(lab_observation_list), 'invalid_obs': res_invalid}

    if verbose:
      self.logger.info("Results:")
      self.logger.info(f"Created {res_stats['valid_obs']+res_stats['invalid_obs']} "
                        "Observation (Laboratory) resources in total")
      if res_stats['invalid_obs']:
        self.logger.warning(f"Detected {res_stats['invalid_obs']} invalid Observation "
                             "(Laboratory) resources being discarded")
      else:
        self.logger.info(f"Detected NO invalid Observation (Laboratory) resources")

    if dest.dtype == 'psql':      
      current_ts = datetime.now()
      new_fhir_bundle.execute(dest)
      ups_res_obs, rm_res_obs = umm_db_lib.get_ups_rm_num_fhir('Obs_lab', current_ts,
                                                               dest.endpoint)
      res_stats_ext = {'ups_obs': ups_res_obs, 'rm_obs': rm_res_obs}
      res_stats.update(res_stats_ext)
      if verbose:
        self.logger.info("Results:")
        self.logger.info(f"Upserted {res_stats['ups_obs']} Observation (laboratory) resources")
    else:
      new_fhir_bundle.execute(dest)

    return res_stats
#!/usr/bin/python3.6

'''Create a database connection
   Arguments: config, db_conn_name, logger
   Returns: none
   Author: Lukas Goetz, Lukas.Goetz@medma.uni-heidelberg.de
   Date: 01-04-2021'''

from sqlalchemy import create_engine

class DBConnectionUMM:
  '''Create DB connection'''

  def __init__(self, config, db_conn_name, logger):
    try:
      self.logger = logger
      host = config[db_conn_name]["host"]
      db = config[db_conn_name]["db"]
      usr = config[db_conn_name]["usr"]
      pwd = config[db_conn_name]["pwd"]
      dbms = config[db_conn_name]["dbsystem"]

      if dbms == "psql":
        engine = create_engine(f"postgresql+psycopg2://{usr}:{pwd}@{host}/{db}",
                                isolation_level="AUTOCOMMIT",
                                connect_args={'connect_timeout': 1})
      else:
        raise ValueError()

      self.engine = engine
      self.host = host
      self.db_conn_name = db_conn_name

    except ValueError as exc:
      self.logger.error(f'''In {__name__}: Other database management system than PostgreSQL
                            is not supported''')
      raise
    except KeyError as exc:
      self.logger.error(f"In {__name__}: Key {exc} not found in dictionary")
      raise
    except Exception as exc:
      self.logger.error(f"In {__name__}: Error occurred during connecting to database ({exc})")
      raise

  def check_db_connection(self):
    try:
      db_con = self.engine.connect()
      db_con.execute("SELECT Version()")
      db_con.close()

      self.logger.info(f"Successfully connected to {self.host} ({self.db_conn_name})")
    except Exception as exc:
      self.logger.error(f"In {__name__}: Error occurred during connecting to database ({exc})")
      raise    

  def create_con(self):
    if hasattr(self, 'engine'):
      self._db_con = self.engine.connect()
      self._db_con.autocommit = True      
      return_val = self._db_con
    else:
      return_val = 0
    return return_val

  def close_con(self):
    if hasattr(self, '_db_con'):
      self._db_con.close()

  def get_engine(self):
    if hasattr(self, 'engine'):
      return self.engine

  def get_pat_num_p21(self, res_type, tbl_names, start_date, end_date):
    if not tbl_names:
      tbl_names = {}
      tbl_names['diag'] = 'rf_med_cov_diagnosis'
      tbl_names['pro'] = 'rf_med_cov_procedure'
      tbl_names['enc'] = 'rf_med_cov_encounter'
      tbl_names['pat'] = 'rf_med_cov_patient'
      tbl_names['lab'] = 'f_med_lab_result'

    if res_type == 'enc':
      sql = f"SELECT COUNT(distinct {tbl_names['enc']}.patient_id) AS pat_num FROM {tbl_names['enc']}"\
            f" INNER JOIN {tbl_names['pat']}"\
            f" ON {tbl_names['enc']}.patient_id = {tbl_names['pat']}.patient_id"\
            f" WHERE {tbl_names['pat']}.patient_birthdate IS NOT NULL AND"\
            f" {tbl_names['enc']}.admission_timestamp IS NOT NULL AND"\
            f" {tbl_names['enc']}.discharge_timestamp IS NOT NULL AND"\
            f" {tbl_names['enc']}.admission_timestamp BETWEEN '{start_date}' AND '{end_date}'"
    elif res_type == 'lab_obs':
      sql = f"SELECT count(distinct {tbl_names['enc']}.patient_id) AS pat_num"\
            f" FROM {tbl_names['enc']}"\
            f" INNER JOIN {tbl_names['lab']}"\
            f" ON {tbl_names['enc']}.encounter_id = {tbl_names['lab']}.encounter_id::int"\
            f" INNER JOIN {tbl_names['pat']}"\
            f" ON {tbl_names['enc']}.patient_id = {tbl_names['pat']}.patient_id"\
            f" WHERE {tbl_names['lab']}.loinc_code <> 'noLoinc' AND"\
            f" {tbl_names['pat']}.patient_birthdate IS NOT NULL AND"\
            f" {tbl_names['enc']}.admission_timestamp IS NOT NULL AND"\
            f" {tbl_names['enc']}.discharge_timestamp IS NOT NULL AND"\
            f" {tbl_names['enc']}.admission_timestamp BETWEEN '{start_date}' AND '{end_date}'"
    elif res_type == 'vent_obs':
      sql = f"SELECT COUNT(distinct {tbl_names['enc']}.patient_id) AS pat_num FROM {tbl_names['enc']}"\
            f" INNER JOIN {tbl_names['pat']}"\
            f" ON {tbl_names['enc']}.patient_id = {tbl_names['pat']}.patient_id"\
            f" WHERE {tbl_names['enc']}.ventilation_hours IS NOT NULL AND"\
            f" {tbl_names['pat']}.patient_birthdate IS NOT NULL AND"\
            f" {tbl_names['enc']}.admission_timestamp IS NOT NULL AND"\
            f" {tbl_names['enc']}.discharge_timestamp IS NOT NULL AND"\
            f" {tbl_names['enc']}.admission_timestamp BETWEEN '{start_date}' AND '{end_date}'"
    elif res_type == 'dia_obs':
      sql = f"SELECT COUNT(distinct {tbl_names['enc']}.patient_id) AS pat_num FROM {tbl_names['enc']}"\
            f" INNER JOIN {tbl_names['pat']}"\
            f" ON {tbl_names['enc']}.patient_id = {tbl_names['pat']}.patient_id"\
            f" WHERE {tbl_names['enc']}.intercurrent_dialyses IS NOT NULL AND"\
            f" {tbl_names['pat']}.patient_birthdate IS NOT NULL AND"\
            f" {tbl_names['enc']}.admission_timestamp IS NOT NULL AND"\
            f" {tbl_names['enc']}.discharge_timestamp IS NOT NULL AND"\
            f" {tbl_names['enc']}.admission_timestamp BETWEEN '{start_date}' AND '{end_date}'"
    elif res_type == 'icu_obs':
      sql = f"SELECT COUNT(distinct {tbl_names['enc']}.patient_id) AS pat_num FROM {tbl_names['enc']}"\
            f" INNER JOIN {tbl_names['pat']}"\
            f" ON {tbl_names['enc']}.patient_id = {tbl_names['pat']}.patient_id"\
            f" WHERE {tbl_names['enc']}.icu_days IS NOT NULL AND"\
            f" {tbl_names['pat']}.patient_birthdate IS NOT NULL AND"\
            f" {tbl_names['enc']}.admission_timestamp IS NOT NULL AND"\
            f" {tbl_names['enc']}.discharge_timestamp IS NOT NULL AND"\
            f" {tbl_names['enc']}.admission_timestamp BETWEEN '{start_date}' AND '{end_date}'"
    else:
      if res_type == 'diag':
        res_type = tbl_names['diag']
      elif res_type == 'pro':
        res_type = tbl_names['pro']
      sql = f"SELECT COUNT(distinct {tbl_names['enc']}.patient_id) AS pat_num FROM {tbl_names['enc']}"\
            f" INNER JOIN {res_type}"\
            f" ON {tbl_names['enc']}.encounter_id = {res_type}.encounter_id"\
            f" INNER JOIN {tbl_names['pat']}"\
            f" ON {tbl_names['enc']}.patient_id = {tbl_names['pat']}.patient_id"\
            f" WHERE {tbl_names['pat']}.patient_birthdate IS NOT NULL AND"\
            f" {tbl_names['enc']}.admission_timestamp IS NOT NULL AND"\
            f" {tbl_names['enc']}.discharge_timestamp IS NOT NULL AND"\
            f" {tbl_names['enc']}.admission_timestamp BETWEEN '{start_date}' AND '{end_date}'"

    db_con = self.engine.connect()
    db_con.autocommit = True
    cur = db_con.execute(sql)
    nrows = cur.fetchone()[0] 
    db_con.close()

    return nrows

  def get_case_num_p21(self, res_type, tbl_names, start_date, end_date):
    if not tbl_names:
      tbl_names = {}
      tbl_names['diag'] = 'rf_med_cov_diagnosis'
      tbl_names['pro'] = 'rf_med_cov_procedure'
      tbl_names['enc'] = 'rf_med_cov_encounter'
      tbl_names['pat'] = 'rf_med_cov_patient'
      tbl_names['lab'] = 'f_med_lab_result'

    if res_type == 'enc':
      sql = f"SELECT COUNT(distinct {tbl_names['enc']}.encounter_id) AS case_num FROM {tbl_names['enc']}"\
            f" INNER JOIN {tbl_names['pat']}"\
            f" ON {tbl_names['enc']}.patient_id = {tbl_names['pat']}.patient_id"\
            f" WHERE {tbl_names['pat']}.patient_birthdate IS NOT NULL AND"\
            f" {tbl_names['enc']}.admission_timestamp IS NOT NULL AND"\
            f" {tbl_names['enc']}.discharge_timestamp IS NOT NULL AND"\
            f" {tbl_names['enc']}.admission_timestamp BETWEEN '{start_date}' AND '{end_date}'"
    elif res_type == 'lab_obs':
      sql = f"SELECT count(distinct {tbl_names['enc']}.encounter_id) AS case_num"\
            f" FROM {tbl_names['enc']}"\
            f" INNER JOIN {tbl_names['lab']}"\
            f" ON {tbl_names['enc']}.encounter_id = {tbl_names['lab']}.encounter_id::int"\
            f" INNER JOIN {tbl_names['pat']}"\
            f" ON {tbl_names['enc']}.patient_id = {tbl_names['pat']}.patient_id"\
            f" WHERE {tbl_names['lab']}.loinc_code <> 'noLoinc' AND"\
            f" {tbl_names['pat']}.patient_birthdate IS NOT NULL AND"\
            f" {tbl_names['enc']}.admission_timestamp IS NOT NULL AND"\
            f" {tbl_names['enc']}.discharge_timestamp IS NOT NULL AND"\
            f" {tbl_names['enc']}.admission_timestamp BETWEEN '{start_date}' AND '{end_date}'"
    elif res_type == 'vent_obs':
      sql = f"SELECT COUNT(distinct {tbl_names['enc']}.encounter_id) AS case_num FROM {tbl_names['enc']}"\
            f" INNER JOIN {tbl_names['pat']}"\
            f" ON {tbl_names['enc']}.patient_id = {tbl_names['pat']}.patient_id"\
            f" WHERE {tbl_names['enc']}.ventilation_hours IS NOT NULL AND"\
            f" {tbl_names['pat']}.patient_birthdate IS NOT NULL AND"\
            f" {tbl_names['enc']}.admission_timestamp IS NOT NULL AND"\
            f" {tbl_names['enc']}.discharge_timestamp IS NOT NULL AND"\
            f" {tbl_names['enc']}.admission_timestamp BETWEEN '{start_date}' AND '{end_date}'"
    elif res_type == 'dia_obs':
      sql = f"SELECT COUNT(distinct {tbl_names['enc']}.encounter_id) AS case_num FROM {tbl_names['enc']}"\
            f" INNER JOIN {tbl_names['pat']}"\
            f" ON {tbl_names['enc']}.patient_id = {tbl_names['pat']}.patient_id"\
            f" WHERE {tbl_names['enc']}.intercurrent_dialyses IS NOT NULL AND"\
            f" {tbl_names['pat']}.patient_birthdate IS NOT NULL AND"\
            f" {tbl_names['enc']}.admission_timestamp IS NOT NULL AND"\
            f" {tbl_names['enc']}.discharge_timestamp IS NOT NULL AND"\
            f" {tbl_names['enc']}.admission_timestamp BETWEEN '{start_date}' AND '{end_date}'"
    elif res_type == 'icu_obs':
      sql = f"SELECT COUNT(distinct {tbl_names['enc']}.encounter_id) AS case_num FROM {tbl_names['enc']}"\
            f" INNER JOIN {tbl_names['pat']}"\
            f" ON {tbl_names['enc']}.patient_id = {tbl_names['pat']}.patient_id"\
            f" WHERE {tbl_names['enc']}.icu_days IS NOT NULL AND"\
            f" {tbl_names['pat']}.patient_birthdate IS NOT NULL AND"\
            f" {tbl_names['enc']}.admission_timestamp IS NOT NULL AND"\
            f" {tbl_names['enc']}.discharge_timestamp IS NOT NULL AND"\
            f" {tbl_names['enc']}.admission_timestamp BETWEEN '{start_date}' AND '{end_date}'"
    else:
      if res_type == 'diag':
        res_type = tbl_names['diag']
      elif res_type == 'pro':
        res_type = tbl_names['pro']

      sql = f"SELECT COUNT(distinct {tbl_names['enc']}.encounter_id) AS case_num FROM {tbl_names['enc']}"\
            f" INNER JOIN {res_type}"\
            f" ON {tbl_names['enc']}.encounter_id = {res_type}.encounter_id"\
            f" INNER JOIN {tbl_names['pat']}"\
            f" ON {tbl_names['enc']}.patient_id = {tbl_names['pat']}.patient_id"\
            f" WHERE {tbl_names['pat']}.patient_birthdate IS NOT NULL AND"\
            f" {tbl_names['enc']}.admission_timestamp IS NOT NULL AND"\
            f" {tbl_names['enc']}.discharge_timestamp IS NOT NULL AND"\
            f" {tbl_names['enc']}.admission_timestamp BETWEEN '{start_date}' AND '{end_date}'"

    db_con = self.engine.connect()
    db_con.autocommit = True
    cur = db_con.execute(sql)
    nrows = cur.fetchone()[0] 
    db_con.close()

    return nrows

  def __del__(self):
    try:
      if hasattr(self, '_db_con'):
        self._db_con.close()
      if hasattr(self, '_db_con'):
        self._db_con.close()        
    except Exception as exc:
      self.logger.error(f"In '{__name__}': Database connection could not be closed ({exc})")

# helper functions
def get_ups_rm_num_fhir(res_type, timestamp, db_con):
  addition = ''
  if res_type == 'Obs_vent':
    res_type = 'Observation'
    addition = "AND data->'code'->'coding'->0->>'code' = '74201-5'"
  elif res_type == 'Obs_dial':
    res_type = 'Observation'
    addition = "AND data->'code'->'coding'->0->>'code' = 'intercurrent-dialysis'"
  elif res_type == 'Obs_icu':
    res_type = 'Observation'
    addition = "AND data->'code'->'coding'->0->>'code' = '74200-7'"
  elif res_type == 'Obs_lufu':
    res_type = 'Observation'
    addition = "AND data->'meta'->>'source' = '#lufu-cwd'"
  elif res_type == 'Obs_lab':
    res_type = 'Observation'
    addition = "AND data->'meta'->>'source' = '#laboratory'"

  sql_upserted = f'''SELECT count(*)
                     FROM stg_fhir_dm.resources_inc
                     WHERE GREATEST(created_at, last_updated_at) >=
                           '{timestamp}' AND type = '{res_type}' AND
                           is_deleted = FALSE {addition}'''                         
  nof_ups = db_con.execute(sql_upserted)
  nof_ups = nof_ups.fetchone()[0]
  sql_rm = f'''SELECT count(*)
               FROM stg_fhir_dm.resources_inc
               WHERE GREATEST(created_at, last_updated_at) >=
                     '{timestamp}' AND type = '{res_type}' AND
                     is_deleted = TRUE {addition}'''
  result = db_con.execute(sql_rm)
  nof_rm = result.fetchone()[0]

  return nof_ups, nof_rm

def get_pat_num_fhir(res_type, db_con):
  if res_type == 'lab_obs':
    sql = f"SELECT COUNT(DISTINCT res2.fhir_id) AS pat_num FROM resources_inc AS res1"\
          f" LEFT JOIN resources_inc AS res2"\
           " ON res1.data -> 'subject' ->> 'reference' = concat('Patient/',res2.fhir_id)"\
          f" WHERE res1.type = 'Observation' AND res2.type = 'Patient' AND"\
           "       (res1.data -> 'meta' ->> 'source')::text = '#laboratory' AND "\
           "        res1.is_deleted = FALSE AND res2.is_deleted = FALSE"
  elif res_type == 'vent_obs' or res_type == 'dia_obs' or res_type == 'icu_obs':

    if res_type == 'vent_obs':
      code = '74201-5'
    elif res_type == 'dia_obs':
      code = 'intercurrent-dialysis'
    else:
      code = '74200-7'

    sql = f"SELECT COUNT(DISTINCT res2.fhir_id) AS pat_num FROM resources_inc AS res1"\
          f" LEFT JOIN resources_inc AS res2"\
           " ON res1.data -> 'subject' ->> 'reference' = concat('Patient/',res2.fhir_id)"\
          f" WHERE res1.type = 'Observation' AND res2.type = 'Patient' AND"\
           "       (res1.data -> 'meta' ->> 'source')::text = '#p21' AND"\
          f"        res1.data -> 'code' -> 'coding' -> 0 ->> 'code' = '{code}' AND "\
           "        res1.is_deleted = FALSE AND res2.is_deleted = FALSE"
  else:
    sql = f"SELECT COUNT(DISTINCT res2.fhir_id) AS pat_num FROM resources_inc AS res1"\
          f" LEFT JOIN resources_inc AS res2"\
           " ON res1.data -> 'subject' ->> 'reference' = concat('Patient/',res2.fhir_id)"\
          f" WHERE res1.type = '{res_type}' AND res2.type = 'Patient' AND "\
           " res1.is_deleted = FALSE AND res2.is_deleted = FALSE"

  result = db_con.execute(sql)
  nrows = result.fetchone()[0]

  return nrows

def get_case_num_fhir(res_type, db_con):
  if res_type == 'lab_obs':
    sql = f"SELECT COUNT(DISTINCT res2.fhir_id) AS case_num FROM resources_inc AS res1"\
          f" LEFT JOIN resources_inc AS res2"\
           " ON res1.data -> 'encounter' ->> 'reference' = concat('Encounter/',res2.fhir_id)"\
          f" WHERE res1.type = 'Observation' AND res2.type = 'Encounter' AND"\
           "       (res1.data -> 'meta' ->> 'source')::text = '#laboratory' AND"\
           "        res1.is_deleted = FALSE AND res2.is_deleted = FALSE"
  elif res_type == 'vent_obs' or res_type == 'dia_obs' or res_type == 'icu_obs':

    if res_type == 'vent_obs':
      code = '74201-5'
    elif res_type == 'dia_obs':
      code = 'intercurrent-dialysis'
    else:
      code = '74200-7'

    sql = f"SELECT COUNT(DISTINCT res2.fhir_id) AS case_num FROM resources_inc AS res1"\
          f" LEFT JOIN resources_inc AS res2"\
           " ON res1.data -> 'encounter' ->> 'reference' = concat('Encounter/',res2.fhir_id)"\
          f" WHERE res1.type = 'Observation' AND res2.type = 'Encounter' AND"\
           "       (res1.data -> 'meta' ->> 'source')::text = '#p21' AND"\
          f"        res1.data -> 'code' -> 'coding' -> 0 ->> 'code' = '{code}' AND "\
           "        res1.is_deleted = FALSE AND res2.is_deleted = FALSE"
  elif res_type == 'sub_enc':
    sql = f"SELECT COUNT(distinct res2.fhir_id) AS case_num FROM resources_inc AS res1"\
          f" LEFT JOIN resources_inc AS res2"\
           " ON res1.data -> 'partOf' ->> 'reference' = concat('Encounter/',res2.fhir_id)"\
           " WHERE res1.type = 'Encounter' AND res2.type = 'Encounter' AND"\
           " res1.data -> 'identifier' -> 0 ->> 'system' ="\
           " 'https://miracum.org/fhir/NamingSystem/identifier/SubEncounterId' AND "\
           " res1.is_deleted = FALSE AND res2.is_deleted = FALSE"
  elif res_type == 'Encounter':
    sql = f"SELECT COUNT(DISTINCT res.fhir_id) AS case_num FROM resources_inc AS res"\
          f" WHERE res.type = 'Encounter'"
  else:
    sql = f"SELECT COUNT(DISTINCT res2.fhir_id) AS case_num FROM resources_inc AS res1"\
          f" LEFT JOIN resources_inc AS res2"\
           " ON res1.data -> 'encounter' ->> 'reference' = concat('Encounter/',res2.fhir_id)"\
          f" WHERE res1.type = '{res_type}' AND res2.type = 'Encounter' AND "\
           " res1.is_deleted = FALSE AND res2.is_deleted = FALSE"

  result = db_con.execute(sql)
  nrows = result.fetchone()[0]

  return nrows
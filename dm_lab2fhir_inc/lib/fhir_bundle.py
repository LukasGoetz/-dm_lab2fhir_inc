#!/usr/bin/python3.6

'''Create FHIR bundle consisting of FHIR resources and send
   them to FHIR DB/ server
   Arguments: logger
   Returns: none
   Author: Lukas Goetz, Lukas.Goetz@medma.uni-heidelberg.de
   Date: 04-19-2020'''

import json
import requests
import sqlalchemy

class FHIRBundle:
  '''Create FHIR bundle consisting of FHIR resources and send
     them to FHIR DB/ server'''

  class UMMDestination:
    def __init__(self, dtype, endpoint):
      self.dtype = dtype
      self.endpoint = endpoint

  def __init__(self, logger):
    self.logger = logger
    self.reset()

  def reset(self):
    self.bundle = {"resourceType": "Bundle", "type": "transaction", "entry": []}
    self.canceled_ids = {'Patient': [], 'Encounter': [], 'Condition': [],
                         'Procedure': [], 'MedicationStatement': [], 'Observation': []}

  def _rm_canceled_encounters(self, enc_id, db_con):
      sql_delete = f'''UPDATE resources_inc
                       SET is_deleted = TRUE
                       WHERE fhir_id = '{enc_id}' AND type = 'Encounter' '''
      db_con.execute(sql_delete)
      sql_update = f'''UPDATE resources_inc
                       SET data = jsonb_set(data, %s, '"UNKNOWN"', TRUE)
                       WHERE data->'encounter'->'reference' = '"Encounter/{enc_id}"' '''
      db_con.execute(sql_update, '''{"encounter","reference"}''')
      sql_update_med = f'''UPDATE resources_inc
                           SET data = jsonb_set(data, %s, '"UNKNOWN"', TRUE)
                           WHERE data->'context'->'reference' = '"Encounter/{enc_id}"' '''
      db_con.execute(sql_update_med, '''{"context","reference"}''')

  def _rm_canceled_patients(self, pat_id, db_con):
    sql_delete = f'''UPDATE resources_inc
                     SET is_deleted = TRUE, last_updated_at = NOW()
                     WHERE fhir_id = '{pat_id}' AND type = 'Patient' '''
    db_con.execute(sql_delete)
    sql_delete_2 = f'''UPDATE resources_inc
                       SET is_deleted = TRUE, last_updated_at = NOW()
                       WHERE data->'subject'->'reference' = '"Patient/{pat_id}"' AND
                       (type = 'Condition' OR type = 'Procedure' OR type = 'Observation') '''
    db_con.execute(sql_delete_2)
    sql_update = f'''UPDATE resources_inc
                     SET data = jsonb_set(data, %s, '"UNKNOWN"', TRUE), last_updated_at = NOW()
                     WHERE data->'subject'->'reference' = '"Patient/{pat_id}"' AND
                     (type = 'Encounter' OR type = 'MedicationStatement') '''
    db_con.execute(sql_update, '''{"subject","reference"}''')

  def _rm_canceled_conditions(self, cond_id, db_con):
    sql_delete = f'''UPDATE resources_inc
                     SET is_deleted = TRUE
                     WHERE fhir_id = '{cond_id}' AND type = 'Condition' '''
    db_con.execute(sql_delete)

    sql_get_index = f'''SELECT index-1
                        FROM resources_inc, jsonb_array_elements(data->'diagnosis')
                             WITH ORDINALITY arr(diag, index)
                        WHERE type = 'Encounter' AND
                              diag->'condition'->'reference' = '"Condition/{cond_id}"' '''
    result = db_con.execute(sql_get_index)
    result = result.fetchone()
    if result:
      sql_update = f'''UPDATE resources_inc
                       SET data = jsonb_set(data, %s, '"UNKNOWN"', false)
                       WHERE type = 'Encounter' '''
      db_con.execute(sql_update, '''{"diagnosis", ''' + str(result[0]) +
                                 ''', "condition", "reference"}''')

  def _rm_canceled_procedures(self, prod_id, db_con):
    sql_delete = f'''UPDATE resources_inc
                     SET is_deleted = TRUE
                     WHERE fhir_id = '{prod_id}' AND type = 'Procedure' '''
    db_con.execute(sql_delete)

  def _rm_canceled_medications(self, med_id, db_con):
    sql_delete = f'''UPDATE resources_inc
                     SET is_deleted = TRUE
                     WHERE fhir_id = '{med_id}' AND type = 'MedicationStatement' '''
    db_con.execute(sql_delete)

  def _rm_canceled_observations(self, obs_id, db_con):
    sql_delete = f'''UPDATE resources_inc
                     SET is_deleted = TRUE
                     WHERE fhir_id = '{obs_id}' AND type = 'Observation' '''
    db_con.execute(sql_delete)

  # Add FHIR resource(s) to bundle
  def add_resources(self, res_list):
    try:
      res_ids = []
      for res in res_list:
        if res.id in res_ids:
          continue
        else:
          res_ids.append(res.id)

        res_entry = {"fullurl": f"{res.resource_type}/{res.id}",
                     "resource": {},
                     "request": {"method": "POST", "url": f"{res.resource_type}",
                                 "ifNoneExist": f"identifier={res.identifier[0].system}|{res.id}"}}
        res_entry['resource'] = res.as_json()
        self.bundle['entry'].append(res_entry)

    except Exception as exc:
      self.logger.error(f"In '{__name__}': FHIR resource(s) could not be added to bundle ({exc})",
                        exc_info=True)
      raise

  def rm_resources(self, type, id):
    self.canceled_ids[type].append(id)

  # Print FHIR bundle as indented json
  def print_as_json(self):
    print(json.dumps(self.bundle, indent=2))

  # Send FHIR bundle to PostgreSQL DB
  def execute(self, dest):
    try:

      if dest.dtype == 'psql':
        # execute upsertions
        ins_val_tup = ()
        cursor = dest.endpoint.raw_connection().cursor()
        self.logger.info(f"Send FHIR resources to FHIR DB for upsert ...")
        for entry in self.bundle['entry']:
          res_id = entry['resource']['id']
          res_type = entry['resource']['resourceType']

          res_body = json.dumps(entry['resource'])
          ins_val_tup = (cursor.mogrify("(%s, %s, %s, %s)",
                                        (res_id, res_type, res_body, 'False')).decode("utf-8"),)

          ins_val_str = ','.join(ins_val_tup)
          dest.endpoint.execute(
            sqlalchemy.text("INSERT INTO stg_fhir_dm.resources_inc (fhir_id, type, data, is_deleted) VALUES " +
                                              ins_val_str +
                                              ''' ON CONFLICT (fhir_id, type) DO UPDATE
                                                  set data = EXCLUDED.data,
                                                      last_updated_at = NOW(),
                                                      is_deleted = false'''))
        # execute deletions
        if (self.canceled_ids['Encounter'] or self.canceled_ids['Patient'] or
            self.canceled_ids['Condition'] or self.canceled_ids['Procedure'] or
            self.canceled_ids['MedicationStatement'] or self.canceled_ids['Observation']):
          self.logger.info(f"Send request to FHIR DB for deletion ...")
          for enc_id in self.canceled_ids['Encounter']:
            self._rm_canceled_encounters(enc_id, dest.endpoint)
          for pat_id in self.canceled_ids['Patient']:
            self._rm_canceled_patients(pat_id, dest.endpoint)
          for cond_id in self.canceled_ids['Condition']:
            self._rm_canceled_conditions(cond_id, dest.endpoint)
          for prod_id in self.canceled_ids['Procedure']:
            self._rm_canceled_procedures(prod_id, dest.endpoint)
          for med_id in self.canceled_ids['MedicationStatement']:
            self._rm_canceled_medications(med_id, dest.endpoint)
          for obs_id in self.canceled_ids['Observation']:
            self._rm_canceled_observations(obs_id, dest.endpoint)
      else:
        headers = {"Content-Type": "application/fhir+json;charset=utf-8"}

        session = requests.Session()
        session.trust_env = False

        response = session.post(dest.endpoint, headers=headers, json=self.bundle)
        response.raise_for_status()
        self.logger.info(f"FHIR bundle was sent to FHIR server")

    except Exception as exc:
      self.logger.error(f"In '{__name__}': FHIR bundle could not be sent to FHIR DB ({exc})",
                        exc_info=True)
      raise

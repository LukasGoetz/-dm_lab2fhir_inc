#!/usr/bin/python3.6

'''Map department table of datamart to FHIR resources of type
   Encounter/ Organization
   Arguments: config
   Returns: FHIR resources
   Author: Lukas Goetz, Lukas.Goetz@medma.uni-heidelberg.de
   Date: 04-19-2020'''

import pandas as pd
from hashlib import sha256
from datetime import datetime
from lib.mii_profiles import (mii_coding, mii_codeableconcept, mii_encounter_abfall,
                              identifier, mii_period, fhirdate, fhirreference, meta,
                              location)
from lib.mii_profiles.fhirabstractbase import FHIRValidationError

#from fhirclient.models import (fhirdate, fhirreference, meta)

class MapperDMDep2Enc:
  '''Map department table of datamart to FHIR resources of type
   Encounter/ Organization'''

  def __init__(self, logger, systems, map_table):
    self.logger = logger
    self.systems = systems
    self.map_table = map_table
    self.data = {}

  def read(self, patient_psn, encounter_psn, dept_p301_code, db_record):
    self.data['patient_psn'] = patient_psn
    self.data['encounter_psn'] = encounter_psn
    self.data['admission_dt'] = db_record.event_begin_timestamp.iloc[0]
    self.data['discharge_dt'] = db_record.event_end_timestamp.iloc[-1]
    self.data['dep_full_code'] = dept_p301_code
    self.data['loc_name_set'] = db_record.unit_name
    self.data['loc_am_ts_set'] = db_record.event_begin_timestamp
    self.data['loc_dc_ts_set'] = db_record.event_end_timestamp    

    concat_elements = []
    concat_elements.append(encounter_psn)
    concat_elements.append(patient_psn)
    concat_elements.append(self.data['dep_full_code'])
    concat_elements.append(str(self.data['admission_dt']))
    concat_elements.append(str(self.data['discharge_dt']))
    self.data['concat_elements'] = ''.join(concat_elements)

  def _dep_name_lookup(self, code):
    df1 = self.map_table

    record1 = df1.loc[df1['code'] == int(code)]
    result = record1['dep_name'].to_string(index=False)

    return result

  def map(self):
    try:
      sub_encounter = mii_encounter_abfall.Encounter()
      id_value = sha256(self.data['concat_elements'].encode('utf-8')).hexdigest()

      sub_encounter.id = id_value
      sub_encounter.identifier = [identifier.Identifier({"use": "usual",
                                                         "system": self.systems['subencounter_id'],
                                                         "value": id_value})]
      sub_encounter.status = "in-progress"
      enc_class_system = "http://terminology.hl7.org/CodeSystem/v3-ActCode"
      sub_encounter.class_fhir = mii_coding.Coding({"system": enc_class_system,
                                                "code": "IMP",
                                                "display": "inpatient encounter"})
      ref = {"reference": f"Patient/{self.data['patient_psn']}"}
      sub_encounter.subject = fhirreference.FHIRReference(jsondict=ref)
      ref = {"reference": f"Encounter/{self.data['encounter_psn']}"}
      sub_encounter.partOf = fhirreference.FHIRReference(jsondict=ref)

      fab_code = self.data['dep_full_code']
      service_type = mii_codeableconcept.CodeableConcept()
      service_type_sys = "http://terminology.hl7.org/CodeSystem/service-type"
      service_type.coding = [mii_coding.Coding({"system": service_type_sys,
                                            "code": fab_code,
                                            "display": self._dep_name_lookup(fab_code)})]
      sub_encounter.serviceType = service_type

      enc_period = mii_period.Period()
      enc_period.start = fhirdate.FHIRDate(datetime.strftime(self.data['admission_dt'],
                                                             '%Y-%m-%dT%H:%M:%S'))
      if not pd.isna(self.data['discharge_dt']):
        sub_encounter.status = "finished"
        enc_period.end = fhirdate.FHIRDate(datetime.strftime(self.data['discharge_dt'],
                                                             '%Y-%m-%dT%H:%M:%S'))
      sub_encounter.period = enc_period

      enc_meta = meta.Meta()
      enc_meta.source = "#sap-ish"
      sub_encounter.meta = enc_meta

      sub_encounter.location = []
      location_list = []
      added_res_loc = 0
      res_loc_invalid = 0
      for loc_name, loc_am_ts, loc_dc_ts in zip(self.data['loc_name_set'], 
                                                self.data['loc_am_ts_set'],
                                                self.data['loc_dc_ts_set']):
        new_location = location.Location()
        id_value = sha256(loc_name.encode('utf-8')).hexdigest()
        new_location.id = id_value
        new_location.identifier = [identifier.Identifier({"use": "usual",
           "system": "https://diz.mii.de/fhir/CodeSystem/TestOrganisationAbteilungen",
           "value": id_value})]
        new_location.name = loc_name

        loc = mii_encounter_abfall.EncounterLocation()
        loc.status = 'active'
        loc_period = mii_period.Period()
        loc_period.start = fhirdate.FHIRDate(datetime.strftime(loc_am_ts,
                                                               '%Y-%m-%dT%H:%M:%S'))
        if not pd.isna(loc_dc_ts):
          loc.status = "completed"
          loc_period.end = fhirdate.FHIRDate(datetime.strftime(loc_dc_ts,
                                                             '%Y-%m-%dT%H:%M:%S'))
        loc.period = loc_period
        ref = {"reference": f"Location/{id_value}"}
        loc.location = fhirreference.FHIRReference(jsondict=ref)
        try:
          loc.as_json()
          sub_encounter.location.append(loc)
          location_list.append(new_location)
          added_res_loc += 1
        except FHIRValidationError:
          self.logger.warning("Validation error for created Location resource "
                           f"(id: {self.data['encounter_psn']})")
          res_loc_invalid += 1

      return sub_encounter, location_list, added_res_loc, res_loc_invalid
    except KeyError as exc:
      self.logger.error(f"In {__name__}: Key {exc} not found in dictionary")
      raise
    #except Exception as exc:
    #  self.logger.error(f"In {__name__}: Error occurred in mapping ({exc})")
    #  raise

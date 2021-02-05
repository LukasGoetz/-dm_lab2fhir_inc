#!/usr/bin/python3.6

'''Map encounter table of datamart to FHIR resources of type
   Encounter
   Arguments: config
   Returns: FHIR resources
   Author: Lukas Goetz, Lukas.Goetz@medma.uni-heidelberg.de
   Date: 04-19-2020'''

import pandas as pd
from datetime import datetime
from lib.mii_profiles import (mii_codeableconcept, mii_coding, mii_encounter_verfall,
                              extension, fhirdate, fhirreference, mii_identifier,
                              mii_period, meta)

class MapperDMEnc2Enc:
  '''Map encounter table of datamart to FHIR resources of type
     Encounter'''

  def __init__(self, logger, systems, map_table):
    self.logger = logger
    self.systems = systems
    self.map_table = map_table
    self.data = {}

  def read(self, ranked_cond_ref, patient_psn, encounter_psn, db_record):
    self.data['ranked_cond_ref'] = ranked_cond_ref
    self.data['patient_psn'] = patient_psn
    self.data['encounter_psn'] = encounter_psn
    self.data['admission_reason'] = db_record.admission_event_reason
    self.data['discharge_reason'] = db_record.discharge_event_reason
    self.data['admission_dt'] = db_record.admission_timestamp
    self.data['discharge_dt'] = db_record.discharge_timestamp

  def _admission_reason_lookup(self, code):
    df1 = self.map_table[0]
    df2 = self.map_table[1]

    record1 = df1.loc[df1['code'] == int(code[:2])]
    element1 = record1['admissionDisplay'].to_string(index=False)
    record2 = df2.loc[df2['code'] == int(code[2:4])]
    element2 = record2['admissionDisplay'].to_string(index=False)
    text = element1[1:]+' -'+element2

    result = []
    result.append(text)

    return result

  def _discharge_reason_lookup(self, code):
    df1 = self.map_table[2]
    df2 = self.map_table[3]

    record1 = df1.loc[df1['code'] == int(code[:2])]
    element1 = record1['dischargeDisplay'].to_string(index=False)
    record2 = df2.loc[df2['code'] == int(code[2])]
    element2 = record2['dischargeDisplay'].to_string(index=False)
    text = element1[1:]+' -'+element2

    result = []
    result.append(text)
    result.append(record1['dischargeDispositionCode'].to_string(index=False))
    result.append(record1['dischargeDispositionDisplay'].to_string(index=False))

    return result

  def map(self):
    try:
      pat_encounter = mii_encounter_verfall.Encounter()
      pat_encounter.id = self.data['encounter_psn']
      identifier_type = mii_codeableconcept.CodeableConcept()
      identifier_type.coding = [mii_coding.Coding({"system": "http://terminology.hl7.org/CodeSystem/v2-0203",
                                                   "code": "VN"})]
      pat_encounter.identifier = [mii_identifier.Identifier({"use": "usual",
                                                         "system": self.systems['encounter_id'],
                                                         "value": self.data['encounter_psn'],
                                                         "assigner":{"reference":"Organization/1111"},
                                                         "type": identifier_type.as_json()})]
      enc_class_system = "http://terminology.hl7.org/CodeSystem/v3-ActCode"
      pat_encounter.class_fhir = mii_coding.Coding({"system": enc_class_system,
                                                    "code": "IMP",
                                                    "display": "inpatient encounter"})
      ref = {"reference": f"Patient/{self.data['patient_psn']}"}
      pat_encounter.subject = fhirreference.FHIRReference(jsondict=ref)

      enc_period = mii_period.Period()
      pat_encounter.status = "in-progress"
      if not pd.isna(self.data['admission_dt']):
        enc_period.start = fhirdate.FHIRDate(datetime.strftime(self.data['admission_dt'],
                                                               '%Y-%m-%dT%H:%M:%S'))
        pat_encounter.period = enc_period
      if not pd.isna(self.data['discharge_dt']):
        enc_period.end = fhirdate.FHIRDate(datetime.strftime(self.data['discharge_dt'],
                                                               '%Y-%m-%dT%H:%M:%S'))
        pat_encounter.status = "finished"

      pat_encounter.extension = []
      if self.data['admission_reason']:
        admission_reason_obj = self._admission_reason_lookup(self.data['admission_reason'])
        admission_reason_coding = mii_coding.Coding({"system": self.systems['admission_reason'],
                                                     "code": self.data['admission_reason'],
                                                     "display": admission_reason_obj[0]})
        admission_ext = extension.Extension({"url": self.systems['admission_reason_url'],
                                             "valueCoding": admission_reason_coding.as_json()})
        pat_encounter.extension.append(admission_ext)

      if self.data['discharge_reason']:
        discharge_reason_obj = self._discharge_reason_lookup(self.data['discharge_reason'])
        discharge_reason_cc = mii_codeableconcept.CodeableConcept()
        discharge_reason_cc.coding = [mii_coding.Coding({"system": self.systems['discharge_reason'],
                                                     "code": self.data['discharge_reason'],
                                                     "display": discharge_reason_obj[0]})]
        discharge_ext = extension.Extension({"url": self.systems['discharge_reason_url'],
                                             "valueCodeableConcept": discharge_reason_cc.as_json()})
        pat_encounter.extension.append(discharge_ext)

        discharge_dispo = mii_codeableconcept.CodeableConcept()
        discharge_dispo_sys = "http://terminology.hl7.org/CodeSystem/discharge-disposition"
        discharge_dispo.coding = [mii_coding.Coding({"system": discharge_dispo_sys,
                                                     "code": discharge_reason_obj[1],
                                                     "display": discharge_reason_obj[2]})]

        hospitalization_encounter = mii_encounter_verfall.EncounterHospitalization()
        hospitalization_encounter.dischargeDisposition = discharge_dispo
        pat_encounter.hospitalization = hospitalization_encounter

      diagnosis_use = mii_codeableconcept.CodeableConcept()
      diag_use_sys = "http://terminology.hl7.org/CodeSystem/diagnosis-role"
      diagnosis_use.coding = [mii_coding.Coding({"system": diag_use_sys,
                                                 "code": "billing"})]

      if self.data['ranked_cond_ref']:
        pat_encounter.diagnosis = []
        for condition_ref in self.data['ranked_cond_ref']:
          cond_diagnosis = mii_encounter_verfall.EncounterDiagnosis()
          cond_diagnosis.condition = condition_ref[0]
          cond_diagnosis.rank = condition_ref[1]
          cond_diagnosis.use = diagnosis_use
          pat_encounter.diagnosis.append(cond_diagnosis)

      enc_meta = meta.Meta()
      enc_meta.source = "#sap-ish"
      pat_encounter.meta = enc_meta

      return pat_encounter

    except KeyError as exc:
      self.logger.error(f"In {__name__}: Key {exc} not found in dictionary")
      raise
    #except Exception as exc:
    #  self.logger.error(f"In {__name__}: Error occurred in mapping ({exc})")
    #  raise

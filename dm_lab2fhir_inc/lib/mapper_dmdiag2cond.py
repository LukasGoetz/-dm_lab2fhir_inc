#!/usr/bin/python3.6

'''Map diagnosis table of datamart to FHIR resources of type
   Condition
   Arguments: config
   Returns: FHIR resources
   Author: Lukas Goetz, Lukas.Goetz@medma.uni-heidelberg.de
   Date: 04-19-2020'''

from hashlib import sha256
import re
import pandas as pd
from datetime import datetime
from lib.mii_profiles import (mii_codeableconcept, mii_coding, mii_condition, coding, codeableconcept,
                              extension, fhirdate, fhirreference, identifier, meta)

class MapperDMDiag2Cond:
  '''Map diagnosis table of datamart to FHIR resources of type
     Condition'''

  def __init__(self, logger, systems):
    self.logger = logger
    self.systems = systems
    self.data = {}

  def read(self, encounter_psn, patient_psn, db_record, logger):
    self.data['encounter_psn'] = encounter_psn
    self.data['diagnosis_nr'] = db_record.diagnosis_nr
    self.data['patient_psn'] = patient_psn
    self.data['admission_dt'] = db_record.diagnosis_documentation_timestamp
    self.data['code'] = db_record.icd_code
    self.data['sec_code'] = db_record.supplementary_icd_code
    self.data['code_ver'] = db_record.icd_id
    self.data['loc'] = db_record.diagnosis_laterality
    self.data['diag_type'] = db_record.principal_diagnosis_flag

    #concat_elements = []
    #concat_elements.append(encounter_psn)
    #concat_elements.append(patient_psn)
    #concat_elements.append(str(admission_dt))

    #record_dict = dict(db_record._asdict())
    #for key, value in record_dict.items():
    #  concat_elements.append(str(value))
    #self.data['concat_elements'] = ''.join(concat_elements)

  def map(self):
    try:
      id_value = str(self.data['encounter_psn']) + '_' + str(self.data['diagnosis_nr'])
      #sha256(self.data['concat_elements'].encode('utf-8')).hexdigest()

      icd_condition = mii_condition.Condition()
      icd_condition.id = id_value
      icd_condition.identifier = [identifier.Identifier({"system": self.systems['condition_id'],
                                                         "value": id_value})]

      clinic_status = codeableconcept.CodeableConcept()
      clinic_status.coding = [coding.Coding({"system":"http://terminology.hl7.org/CodeSystem/condition-clinical",
                                                "code":"active"})]
      icd_condition.clinicalStatus = clinic_status

      full_code = self.data['code']
      if self.data['sec_code']:
        full_code = self.data['code'] + " " + self.data['sec_code']
      condition_coding = mii_coding.Coding()
      condition_coding.system = "http://fhir.de/CodeSystem/dimdi/icd-10-gm"
      condition_coding.code = full_code
      condition_coding.version = self.data['code_ver']

      if self.data['sec_code']:
        main_cross_ext = extension.Extension()
        main_cross_ext.url = "http://fhir.de/StructureDefinition/icd-10-gm-haupt-kreuz"
        main_cross_ext.valueCoding = coding.Coding({"system": condition_coding.system,
                                                    "version": self.data['code_ver'],
                                                    "code": re.sub(r"[+†]", "", self.data['code'])})
        condition_coding.extension = [main_cross_ext]

        if '!' in self.data['sec_code']:
          excl_ext = extension.Extension()
          excl_ext.url = "http://fhir.de/StructureDefinition/icd-10-gm-ausrufezeichen"
          excl_ext.valueCoding = coding.Coding({"system": condition_coding.system,
                                                "version": self.data['code_ver'],
                                                "code": re.sub(r"!", "", self.data['sec_code'])})
          condition_coding.extension.append(excl_ext)

        if '*' in self.data['sec_code']:
          star_ext = extension.Extension()
          star_ext.url = "http://fhir.de/StructureDefinition/icd-10-gm-stern"
          star_ext.valueCoding = coding.Coding({"system": condition_coding.system,
                                                "version": self.data['code_ver'],
                                                "code": re.sub(r"\*", "", self.data['sec_code'])})
          condition_coding.extension.append(star_ext)

      condition_code = mii_codeableconcept.CodeableConcept()
      condition_code.coding = [condition_coding]
      icd_condition.code = condition_code

      # todo: what value of the data mart should be mapped?
      icd_condition.onsetString = '2002-01-01'

      ref = {"reference": f"Encounter/{self.data['encounter_psn']}"}
      icd_condition.encounter = fhirreference.FHIRReference(jsondict=ref)
      ref = {"reference": f"Patient/{self.data['patient_psn']}"}
      icd_condition.subject = fhirreference.FHIRReference(jsondict=ref)

      if not pd.isna(self.data['admission_dt']):
        icd_condition.recordedDate = fhirdate.FHIRDate(datetime.strftime(self.data['admission_dt'],
                                                                       '%Y-%m-%dT%H:%M:%S'))

      if self.data['loc']:
        icd_condition.bodySite = []
        if self.data['loc'] == "L" or self.data['loc'] == "B":
          left_body_part = mii_codeableconcept.CodeableConcept()
          icd_loc_sys = "http://fhir.de/CodeSystem/kbv/s_icd_seitenlokalisation"
          left_body_part.coding = [mii_coding.Coding({"system": icd_loc_sys,
                                                  "code": "L",
                                                  "display": "links"})]
          icd_condition.bodySite.append(left_body_part)

        if self.data['loc'] == "R" or self.data['loc'] == "B":
          right_body_part = mii_codeableconcept.CodeableConcept()
          icd_loc_sys = "http://fhir.de/CodeSystem/kbv/s_icd_seitenlokalisation"
          right_body_part.coding = [mii_coding.Coding({"system": icd_loc_sys,
                                                   "code": "R",
                                                   "display": "rechts"})]
          icd_condition.bodySite.append(right_body_part)

      rank = 2
      if self.data['diag_type'] == 1:
        rank = 1

      icd_meta = meta.Meta()
      icd_meta.source = "#sap-ish"
      icd_condition.meta = icd_meta

      return [icd_condition, rank]

    except KeyError as exc:
      self.logger.error(f"In {__name__}: Key {exc} not found in dictionary")
      raise
    #except Exception as exc:
    #  self.logger.error(f"In {__name__}: Error occurred in mapping ({exc})")
    #  raise

#!/usr/bin/python3.6

'''Map lab data to FHIR resources of type Observation
   Arguments: config
   Returns: FHIR resources
   Author: Hee Eun Kim, HeeEun.Kim@medma.uni-heidelberg.de
           Lukas Goetz, Lukas.Goetz@medma.uni-heidelberg.de
   Date: 12-03-2020'''

import json
from datetime import datetime
from hashlib import sha256
import pandas as pd
import requests
from lib.mii_profiles.fhirabstractbase import FHIRValidationError
from lib.mii_profiles import (codeableconcept, mii_codeableconcept, coding,
                              mii_coding, fhirdate, fhirreference,
                              mii_identifier, mii_observation, quantity,
                              mii_quantity, meta)
from urllib3.exceptions import HTTPError

class MapperDMLab2Obs:
  '''Map lab data to FHIR resources of type Observation'''

  def __init__(self, logger, systems, loinc_url):
    self.logger = logger
    self.systems = systems
    self.data = {}
    self.loinc_url = loinc_url

  def read(self, encounter_psn, patient_psn, db_record):
    self.data['encounter_psn'] = encounter_psn
    self.data['patient_psn'] = patient_psn
    self.data['loinc_code'] = db_record.loinc_code
    self.data['value_num'] = db_record.result_value_num
    self.data['value_text'] = db_record.result_value
    self.data['value_comp'] = db_record.result_value_comparator
    self.data['value_unit'] = db_record.result_unit
    self.data['ref_low_num'] = db_record.result_reference_low
    self.data['ref_high_num'] = db_record.result_reference_high
    self.data['method'] = str(db_record.method_id)
    self.data['int_flag'] = db_record.result_interpretation_flag
    self.data['collect_ts'] = db_record.collection_timestamp
    self.data['result_id'] = db_record.result_id

    #concat_elements = []
    #concat_elements.append(encounter_psn)
    #concat_elements.append(patient_psn)

    #record_dict = dict(db_record._asdict())
    #for key, value in record_dict.items():
    #  concat_elements.append(str(value))
    #elf.data['concat_elements'] = ''.join(concat_elements)

  def _convert_loinc(self):
    try:
      headers = {"Content-Type": "application/json;charset=utf-8"}
      body = [{"loinc": self.data['loinc_code'], "unit": self.data['value_unit'],
               "value": self.data['value_num']}]

      session = requests.Session()
      session.trust_env = False
      response = session.post(self.loinc_url, headers=headers, json=body)
      response.raise_for_status()

      return(json.loads(response.content))
    except HTTPError as exc:
      self.logger.error(f"In {__name__}: Loin code could not be converted ({exc})")
      self.logger.error(f"In {__name__}: Response: {response.json()}")
      raise

  def map(self):
    try:
      id_value = str(self.data['result_id']) #2sha256(self.data['concat_elements'].encode('utf-8')).hexdigest()

      lab_observation = mii_observation.Observation()
      lab_observation.id = id_value
      identifier_type = mii_codeableconcept.CodeableConcept()
      identifier_type.coding = [mii_coding.Coding({
                                "system": "http://terminology.hl7.org/CodeSystem/v2-0203",
                                "code": "MR"})]
      lab_observation.identifier = [mii_identifier.Identifier({
                                     "system": self.systems['lab_id'],
                                     "value": id_value,
                                     "assigner":{"reference":"Organization/1111"},
                                     "type": identifier_type.as_json()})]
      ref = {"reference": f"Encounter/{self.data['encounter_psn']}"}
      lab_observation.encounter = fhirreference.FHIRReference(jsondict=ref)
      ref = {"reference": f"Patient/{self.data['patient_psn']}"}
      lab_observation.subject = fhirreference.FHIRReference(jsondict=ref)
      lab_observation.status = "final"

      if not pd.isna(self.data['collect_ts']):
        collect_ts_fhir = datetime.strftime(self.data['collect_ts'], '%Y-%m-%dT%H:%M:%S')
        lab_observation.effectiveDateTime = fhirdate.FHIRDate(collect_ts_fhir)

      if not pd.isna(self.data['value_num']):
        if (self.data['loinc_code'] and self.data['loinc_code'] != 'noLoinc' and
            self.data['value_unit']):
          if self.data['value_unit'] == '10E12/L':
            self.data['value_unit'] = '10*6/uL'
          elif self.data['value_unit'] == '10E9/L':
            self.data['value_unit'] = '10*3/uL'
          elif self.data['value_unit'] == 'mE/l':
            self.data['value_unit'] = 'm[IU]/L'
          elif (self.data['value_unit'] == 'ug/l' or
                self.data['value_unit'] == 'µg/l'):
            self.data['value_unit'] = 'ng/mL'

          if self.loinc_url:
            result = self._convert_loinc()
            #print(f"{self.data['loinc_code']} {self.data['value_num']} {self.data['value_unit']}")
            #print(result)

            if result and isinstance(result, list) and not 'error' in result[0]:
              if result[0]['loinc'] and result[0]['value'] and result[0]['unit']:
                self.data['loinc_code'] = result[0]['loinc']
                self.data['value_num'] = result[0]['value']
                self.data['value_unit'] = result[0]['unit']
                self.data['value_unit'] = self.data['value_unit'].replace("'", "''")
          #else:
          #  print(f"{self.data['loinc_code']} {self.data['value_num']} {self.data['value_unit']}")
          #  print(result)

        if (self.data['value_comp'] == '!=' or self.data['value_comp'] == '=' or
            self.data['value_comp'] == '=='):
          self.data['value_comp'] = None

        lab_quantity = mii_quantity.Quantity({"value": self.data['value_num'],
                                              "unit": self.data['value_unit'],
                                              "comparator": self.data['value_comp'],
                                              "system": "http://unitsofmeasure.org",
                                              "code": self.data['value_unit']})
        lab_observation.valueQuantity = lab_quantity
      elif self.data['value_text']:
        lab_observation.valueString = self.data['value_text']

      lab_code = codeableconcept.CodeableConcept()
      lab_code.coding = [coding.Coding({"system": "http://loinc.org",
                                        "code": self.data['loinc_code']})]
      lab_observation.code = lab_code

      if not pd.isna(self.data['ref_high_num']) and not pd.isna(self.data['ref_low_num']):
        reference_range = mii_observation.ObservationReferenceRange()
        reference_range.high = quantity.Quantity({"value": self.data['ref_high_num'],
                                                  "unit": self.data['value_unit'],
                                                  "system": "http://unitsofmeasure.org",
                                                  "code": self.data['value_unit']})
        reference_range.low = quantity.Quantity({"value": self.data['ref_low_num'],
                                                 "unit": self.data['value_unit'],
                                                 "system": "http://unitsofmeasure.org",
                                                 "code": self.data['value_unit']})
        lab_observation.referenceRange = [reference_range]

      lab_method = codeableconcept.CodeableConcept()
      lab_method.coding = [coding.Coding({"system": "http://methodConcept",
                                          "display": self.data['method']})]
      lab_observation.method = lab_method

      lab_meta = meta.Meta()
      lab_meta.source = "#laboratory"
      lab_observation.meta = lab_meta

      lab_cat = mii_codeableconcept.CodeableConcept()
      lab_cat_sys = "http://terminology.hl7.org/CodeSystem/observation-category"
      lab_cat.coding = [mii_coding.Coding({"system": lab_cat_sys,
                                       "display": "Laboratory",
                                       "code": "laboratory"})]
      lab_observation.category = lab_cat

      #lab_annotation = annotation_model.Annotation({"text": note})
      #lab_observation.note = [lab_annotation]

      if self.data['int_flag']:
        if self.data['int_flag'] == "N":
          int_flag_long = "Normal"
        elif self.data['int_flag'] == "L":
          int_flag_long = "Low"
        elif self.data['int_flag'] == "H":
          int_flag_long = "High"
        else:
          int_flag_long = "Unknown"

        lab_interpretation = codeableconcept.CodeableConcept()
        lab_int_sys = "http://terminology.hl7.org/CodeSystem/v3-ObservationInterpretation"
        lab_interpretation.coding = [coding.Coding({"system": lab_int_sys,
                                                    "code": self.data['int_flag'],
                                                    "display": int_flag_long})]
        lab_observation.interpretation = [lab_interpretation]

      return lab_observation
    except KeyError as exc:
      self.logger.error(f"In {__name__}: Key {exc} not found in dictionary")
      raise
    except FHIRValidationError:
      raise
    #except Exception as exc:
    #  self.logger.error(f"In {__name__}: Error occurred in mapping ({exc})")
    #  raise

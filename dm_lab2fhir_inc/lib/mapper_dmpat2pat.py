#!/usr/bin/python3.6

'''Map patient table of datamart to FHIR resources of type
   Patient
   Arguments: config
   Returns: FHIR resources
   Author: Lukas Goetz, Lukas.Goetz@medma.uni-heidelberg.de
   Date: 04-19-2020'''

from datetime import date
import pandas as pd
from lib.mii_profiles import (mii_address, mii_codeableconcept, mii_coding,
                              fhirdate, mii_identifier_pat, mii_patient, meta,
                              mii_humanname)

class MapperDMPat2Pat:
  '''Map patient table of datamart to FHIR resources of type
     Patient'''

  def __init__(self, logger, systems):
    self.logger = logger
    self.systems = systems
    self.data = {}

  def read(self, patient_psn, db_record):
    self.data['patient_psn'] = patient_psn
    self.data['insurance_id'] = db_record.patient_insurance_identifier
    self.data['gender'] = ''
    if db_record.patient_sex:
      self.data['gender'] = db_record.patient_sex.lower()
    self.data['birth_date'] = db_record.patient_birthdate
    self.data['deceased_flag'] = db_record.patient_deceased_flag
    self.data['city'] = db_record.patient_address_city
    self.data['street'] = db_record.patient_address_street
    self.data['country'] = db_record.patient_address_countrycode
    self.data['postal_code'] = db_record.patient_address_zipcode
    self.data['family_name'] = db_record.patient_lastname
    self.data['given_name'] = db_record.patient_firstname

  def map(self):
    try:
      inpatient = mii_patient.Patient()
      inpatient.id = self.data['patient_psn']
      identifier_type = mii_codeableconcept.CodeableConcept()      
      identifier_type.coding = [mii_coding.Coding({"system": "http://terminology.hl7.org/CodeSystem/v2-0203",
                                                   "code": "MR"})]
      inpatient.identifier = [mii_identifier_pat.Identifier({"use": "official",
                                                     "system": self.systems['patient_id'],
                                                     "value": self.data['patient_psn'],
                                                     "assigner":{"reference":"Organization/1111"},
                                                     "type": identifier_type.as_json()})]

      if self.data['insurance_id']:
        insurance_id_type = mii_codeableconcept.CodeableConcept()
        insurance_type_sys = "http://fhir.de/CodeSystem/identifier-type-de-basis"
        insurance_id_type.coding = [mii_coding.Coding({"system": insurance_type_sys,
                                                   "code": "GKV",
                                                   "display": "Gesetzliche Krankenversicherung"})]
        gkv_sys = "http://fhir.de/NamingSystem/gkv/kvid-10"
        identifier_type.coding = [mii_coding.Coding({"system": "http://fhir.de/CodeSystem/identifier-type-de-basi",
                                                     "code": "GKV"})]
        inpatient.identifier.append(mii_identifier_pat.Identifier({"use": "official",
                                                           "system": gkv_sys,
                                                           "type": insurance_id_type.as_json(),
                                                           "value": self.data['insurance_id'],
                                                           "assigner":{"reference":"Organization/1111"}}))

      if self.data['gender'] == 'm':
        gender_code = "male"
      elif self.data['gender'] == 'w':
        gender_code = "female"
      elif self.data['gender'] == 'x':
        gender_code = "other"
      else:
        gender_code = "unknown"
      inpatient.gender = gender_code

      inpatient.birthDate = fhirdate.FHIRDate(str(self.data['birth_date']))

      inpatient.deceasedBoolean = False
      if self.data['deceased_flag']:
        inpatient.deceasedBoolean = True

      inpatient.address = mii_address.Address({"city": self.data['city'],
                                               "postalCode": self.data['postal_code'],
                                               "line": self.data['street'],
                                               "country": self.data['country'],
                                               "type": ""})

      inpatient.name = [mii_humanname.HumanName({"use": 'official',
                                                 "family": self.data['family_name'],
                                                 "given": [self.data['given_name']]})]


      pat_meta = meta.Meta()
      pat_meta.source = "#sap-ish"
      inpatient.meta = pat_meta
      
      return inpatient

    except KeyError as exc:
      self.logger.error(f"In {__name__}: Key {exc} not found in dictionary")
      raise
    #except Exception as exc:
    #  self.logger.error(f"In {__name__}: Error occurred in mapping ({exc})")
    #  raise

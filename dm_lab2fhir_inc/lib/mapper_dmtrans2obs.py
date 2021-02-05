#!/usr/bin/python3.6

'''Map encounter table of datamart to FHIR resources of type
   Observation (Ventilation, Dialysis)
   Arguments: config
   Returns: FHIR resources
   Author: Lukas Goetz, Lukas.Goetz@medma.uni-heidelberg.de
   Date: 12-03-2020'''

from datetime import datetime
import pandas as pd
from lib.mii_profiles.fhirabstractbase import FHIRValidationError
from lib.mii_profiles import (codeableconcept, coding, fhirdate, fhirreference,
                              miracum_codeableconcept, miracum_coding,
                              identifier, miracum_observation, period,
                              miracum_quantity, meta)

class MapperDMTrans2Obs:
  '''Map transfer table of datamart to FHIR resources of type
     Observation (Ventilation, Dialysis)'''

  def __init__(self, logger, systems):
    self.logger = logger
    self.systems = systems
    self.data = {}

  def read(self, encounter_psn, patient_psn, db_record):
    self.data['encounter_psn'] = encounter_psn
    self.data['patient_psn'] = patient_psn
    if not pd.isna(db_record.intercurrent_dialyses):
      self.data['dialysis_hours'] = int(db_record.intercurrent_dialyses)
    else:
      self.data['dialysis_hours'] = None
    self.data['admission_dt'] = db_record.admission_timestamp
    self.data['discharge_dt'] = db_record.discharge_timestamp
    if not pd.isna(db_record.icu_days):
      self.data['icu_days'] = round(db_record.icu_days, 2)
    else:
      self.data['icu_days'] = None

  def map(self):
    try:
      obs_status = "preliminary"
      obs_meta = meta.Meta()
      obs_meta.source = "#sap-ish"

      start_dt = None
      end_dt = None
      if not pd.isna(self.data['admission_dt']):
        start_dt = fhirdate.FHIRDate(datetime.strftime(self.data['admission_dt'],
                                                               '%Y-%m-%dT%H:%M:%S'))
        if not pd.isna(self.data['discharge_dt']):
          end_dt = fhirdate.FHIRDate(datetime.strftime(self.data['discharge_dt'],
                                                               '%Y-%m-%dT%H:%M:%S'))
          obs_status = "final"
      obs_period = period.Period()
      obs_period.start = start_dt
      obs_period.end = end_dt

      id_value = f"{self.data['encounter_psn']}-dia"
      dialysis_observation = miracum_observation.Observation()
      dialysis_observation.id = id_value
      dialysis_sys = self.systems['p21obs_id']
      dialysis_observation.identifier = [identifier.Identifier({"system": dialysis_sys,
                                                                "value": id_value})]
      ref = {"reference": f"Encounter/{self.data['encounter_psn']}"}
      dialysis_observation.encounter = fhirreference.FHIRReference(jsondict=ref)
      ref = {"reference": f"Patient/{self.data['patient_psn']}"}
      dialysis_observation.subject = fhirreference.FHIRReference(jsondict=ref)

      dialysis_code = miracum_codeableconcept.CodeableConcept()
      obs_code_sys = "https://miracum.org/fhir/CodeSystem/core/observations"
      dialysis_code.text = "Intercurrent dialysis"
      dialysis_code.coding = [miracum_coding.Coding({"system": obs_code_sys,
                                                     "code": "intercurrent-dialysis",
                                                     "display": "Intercurrent dialysis"})]
      dialysis_observation.code = dialysis_code
      dialysis_observation.valueInteger = self.data['dialysis_hours']

      dialysis_observation.status = obs_status
      dialysis_observation.effectivePeriod = obs_period
      dialysis_observation.meta = obs_meta

      id_value = f"{self.data['encounter_psn']}-icu"
      icu_observation = miracum_observation.Observation()
      icu_observation.id = id_value
      icu_sys = self.systems['p21obs_id']
      icu_observation.identifier = [identifier.Identifier({"system": icu_sys,
                                                           "value": id_value})]
      ref = {"reference": f"Encounter/{self.data['encounter_psn']}"}
      icu_observation.encounter = fhirreference.FHIRReference(jsondict=ref)
      ref = {"reference": f"Patient/{self.data['patient_psn']}"}
      icu_observation.subject = fhirreference.FHIRReference(jsondict=ref)

      icu_code = miracum_codeableconcept.CodeableConcept()
      icu_code.text = "Days in intensive care unit"
      icu_code.coding = [miracum_coding.Coding({"system": 'http://loinc.org',
                                                "code": "74200-7",
                                                "display": "Days in intensive care unit",
                                                "version": "2.46"})]
      icu_observation.code = icu_code
      if self.data['icu_days']:
        icu_observation.valueQuantity = miracum_quantity.Quantity({"value": self.data['icu_days'],
                                                          "unit": "d",
                                                          "system": "http://unitsofmeasure.org",
                                                          "code": "d"})
      icu_observation.status = obs_status
      icu_observation.effectivePeriod = obs_period
      icu_observation.meta = obs_meta

      return [dialysis_observation, icu_observation]
    except KeyError as exc:
      self.logger.error(f"In {__name__}: Key {exc} not found in dictionary")
      raise
    except FHIRValidationError:
      raise
    #except Exception as exc:
    #  self.logger.error(f"In {__name__}: Error occurred in mapping ({exc})")
    #  raise

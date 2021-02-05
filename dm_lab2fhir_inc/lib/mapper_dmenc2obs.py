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
from lib.mii_profiles import (miracum_codeableconcept, codeableconcept, miracum_coding, coding,
                              fhirreference, identifier, miracum_observation, period,
                              miracum_quantity, meta, fhirdate)

class MapperDMEnc2Obs:
  '''Map encounter table of datamart to FHIR resources of type
     Observation (Ventilation, Dialysis)'''

  def __init__(self, logger, systems):
    self.logger = logger
    self.systems = systems
    self.data = {}

  def read(self, encounter_psn, patient_psn, db_record):
    self.data['encounter_psn'] = encounter_psn
    self.data['patient_psn'] = patient_psn
    if not pd.isna(db_record.ventilation_hours):
      self.data['vent_days'] = round(int(db_record.ventilation_hours)/ 24, 2)
    else:
      self.data['vent_days'] = None
    self.data['admission_dt'] = db_record.admission_timestamp
    self.data['discharge_dt'] = db_record.discharge_timestamp

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

      id_value = f"{self.data['encounter_psn']}-vent"
      vent_observation = miracum_observation.Observation()
      vent_observation.id = id_value
      vent_observation.identifier = [identifier.Identifier({"system": self.systems['p21obs_id'],
                                                            "value": id_value})]

      ref = {"reference": f"Encounter/{self.data['encounter_psn']}"}
      vent_observation.encounter = fhirreference.FHIRReference(jsondict=ref)
      ref = {"reference": f"Patient/{self.data['patient_psn']}"}
      vent_observation.subject = fhirreference.FHIRReference(jsondict=ref)

      ventilation_code = miracum_codeableconcept.CodeableConcept()
      ventilation_code.text = "Days on Ventilator"
      ventilation_code.coding = [miracum_coding.Coding({"system": "http://loinc.org",
                                                        "code": "74201-5",
                                                        "display": "Days on Ventilator"})]
      vent_observation.code = ventilation_code
      if self.data['vent_days']:
        vent_observation.valueQuantity = miracum_quantity.Quantity({
                                                          "value": self.data['vent_days'],
                                                          "unit": "d",
                                                          "system": "http://unitsofmeasure.org",
                                                          "code": "d"})
      vent_observation.status = obs_status
      vent_observation.effectivePeriod = obs_period
      vent_observation.meta = obs_meta

      return vent_observation

    except KeyError as exc:
      self.logger.error(f"In {__name__}: Key {exc} not found in dictionary")
      raise
    except FHIRValidationError:
      raise
    #except Exception as exc:
    #  self.logger.error(f"In {__name__}: Error occurred in mapping ({exc})")
    #  raise

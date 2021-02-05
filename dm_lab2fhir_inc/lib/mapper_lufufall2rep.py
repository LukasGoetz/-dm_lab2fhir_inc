#!/usr/bin/python3.6

'''Map lufu table data set to FHIR resources of type
   DiagnosticReport
   Arguments: config
   Returns: FHIR resources
   Author: Jan Scheer, jan.scheer@medma.uni-heidelberg.de
   Date: 06-23-2020'''
from _sha256 import sha256
from datetime import datetime

import pandas as pd
from lib.mii_profiles import (codeableconcept, coding, fhirdate, fhirreference,
                              identifier, observation, diagnosticreport, period,
                              quantity, meta)

class MapperLuFuFall2Rep:
  '''Map lufu table data set to FHIR resources of type
   DiagnosticReport'''

  def __init__(self, logger, systems):
    self.logger = logger
    self.systems = systems
    self.data = {}


  def read(self, encounter_psn, patient_psn, db_record):

    self.data['encounter_psn'] = encounter_psn
    self.data['patient_psn'] = patient_psn

    self.data['untersuchung_id'] = db_record['untersuchung_id']
    self.data['untersuchungsdatum'] = db_record['untersuchungsdatum']
    self.data['untersuchungsuhrzeit'] = db_record['untersuchungsuhrzeit']
    self.data['untersuchungsart'] = db_record['untersuchungsart']

    self.data['zuweiser'] = db_record['zuweiser']
    #self.data['ausfuehrende_stelle'] = db_record['ausfuehrende_stelle']
    self.data['sendedatum'] = db_record['sendedatum']
    self.data['untersuchung_status'] = db_record['untersuchung_status']

    self.data['performer_1'] = db_record['performer_1']
    self.data['performer_2'] = db_record['performer_2']
    self.data['aufenthalt'] = db_record['aufenthalt']
    self.data['versicherungsart'] = db_record['versicherungsart']
    self.data['gender'] = db_record['gender']
    self.data['height_cm'] = db_record['height_cm']
    self.data['weight_kg'] = db_record['weight_kg']
    self.data['beurteilung'] = db_record['beurteilung']
    self.data['anmerkung'] = db_record['anmerkung']
    self.data['empfehlung'] = db_record['empfehlung']

    concat_elements = []
    concat_elements.append(encounter_psn)
    concat_elements.append(patient_psn)

    record_dict = dict(db_record)
    for key, value in record_dict.items():
      concat_elements.append(str(value))
    self.data['concat_elements'] = ''.join(concat_elements)

  def map(self):
    try:
      id_value = sha256(self.data['concat_elements'].encode('utf-8')).hexdigest()

      lufu_diagnostic_report = diagnosticreport.DiagnosticReport();
      lufu_diagnostic_report.id = id_value
      lufu_diagnostic_report_identifier = self.data['untersuchung_id']
      lufu_diagnostic_report.identifier = [identifier.Identifier({"use": "official",
                                                                  "system": self.systems['lufu_obs_id'],
                                                                  "value": str(lufu_diagnostic_report_identifier)})]
      dt_string = self.data['untersuchungsdatum']+" "+self.data['untersuchungsuhrzeit']
      lufu_report_dt = datetime.strftime(datetime.strptime(dt_string, "%Y-%m-%d %H:%M:%S"), '%Y-%m-%dT%H:%M:%S')
      lufu_diagnostic_report_date = fhirdate.FHIRDate(lufu_report_dt)

      lufu_diagnostic_report.effectiveDateTime = lufu_diagnostic_report_date

      ref = {"reference": f"Patient/{self.data['patient_psn']}"}
      lufu_diagnostic_report.subject = fhirreference.FHIRReference(jsondict=ref)
      ref = {"reference": f"Encounter/{self.data['encounter_psn']}"}
      lufu_diagnostic_report.encounter = fhirreference.FHIRReference(jsondict=ref)

      rep_code_system_cc = codeableconcept.CodeableConcept()
      rep_code_system_cc.coding = [coding.Coding({"system": "https://www.dimdi.de/static/de/klassifikationen/ops",
                     "code": "1-71",
                     "display": "Pneumologische Funktionsuntersuchunge"})]

      rep_status = 'final'
      lufu_diagnostic_report.code = rep_code_system_cc
      lufu_diagnostic_report.status = rep_status
      conclusion_str = ""
      if self.data['beurteilung'] is not None and self.data['beurteilung'] != "":
        conclusion_str = conclusion_str + "Beurteilung: "+self.data['beurteilung']
      if self.data['anmerkung'] is not None and self.data['anmerkung'] != "":
        conclusion_str = conclusion_str + "|\n Anmerkung: "+self.data['anmerkung']
      if self.data['empfehlung'] is not None and self.data['empfehlung'] != "":
        conclusion_str = conclusion_str + " |\n Empfehlung: "+self.data['empfehlung']

      lufu_diagnostic_report.conclusion = conclusion_str

      return lufu_diagnostic_report
    except KeyError as exc:
      self.logger.error(f"In {__name__}: Key {exc} not found in dictionary")
      raise
    except Exception as exc:
      self.logger.error(f"In {__name__}: Error occurred in mapping ({exc})")
      raise


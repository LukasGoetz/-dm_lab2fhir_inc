#!/usr/bin/python3.6

'''Map lufu table data set to FHIR resources of type
   Procedure
   Arguments: config
   Returns: FHIR resources
   Author: Jan Scheer, jan.scheer@medma.uni-heidelberg.de
   Date: 07-15-2020'''
from _sha256 import sha256
from datetime import datetime

import pandas as pd
from lib.mii_profiles import (codeableconcept, coding, fhirdate, fhirreference,
                              identifier, observation, procedure, period, quantity, meta)


class MapperLuFuFall2Proc:
  '''Map lufu table data set to FHIR resources of type
   Procedure'''

  def __init__(self, logger, systems, procedure_mapper):
    self.logger = logger
    self.systems = systems
    self.data = {}
    self.procedure_mapper = procedure_mapper


  def read(self, encounter_psn, patient_psn, db_record):

    self.data['encounter_psn'] = encounter_psn
    self.data['patient_psn'] = patient_psn

    self.data['untersuchung_id'] = db_record['untersuchung_id']
    self.data['untersuchungsdatum'] = db_record['untersuchungsdatum']
    self.data['untersuchungsart'] = db_record['untersuchungsart']

    self.data['zuweiser'] = db_record['zuweiser']
    #self.data['ausfuehrende_stelle'] = db_record['ausfuehrende_stelle']
    self.data['sendedatum'] = db_record['sendedatum']
    self.data['untersuchung_status'] = db_record['untersuchung_status']

    self.data['untersucher1'] = db_record['untersucher1']
    self.data['untersucher2'] = db_record['untersucher2']
    self.data['aufenthalt'] = db_record['aufenthalt']
    self.data['versicherungsart'] = db_record['versicherungsart']
    self.data['geschlecht'] = db_record['geschlecht']
    self.data['groeße_cm'] = db_record['größe_cm']
    self.data['gewicht_kg'] = db_record['gewicht_kg']
    self.data['beurteilung'] = db_record['beurteilung']
    self.data['anmerkung'] = db_record['anmerkung']
    self.data['empfehlung'] = db_record['empfehlung']

    concat_elements = []
    concat_elements.append(encounter_psn)
    concat_elements.append(patient_psn)

    record_dict = dict(db_record)
    for key, value in record_dict.items():
      if key == 'quelldatenjahr':
        continue
      concat_elements.append(str(value))
    self.data['concat_elements'] = ''.join(concat_elements)

  def map(self, lufu_section):
    try:
      ops_code = 'No OPS Code'
      ops_text = 'No OPS Code Text'

      if lufu_section == 'B' :
        ops_code = '1-710'
        ops_text = self.procedure_mapper.codeLookup(ops_code)
      else:
        ops_code = '1-71'
        ops_text = self.procedure_mapper.codeLookup(ops_code)

      id_value = sha256((str(lufu_section)+self.data['concat_elements']).encode('utf-8')).hexdigest()

      lufu_procedure = procedure.Procedure();

      #lufu_procedure_system = ''
      lufu_procedure_identifier = id_value
      lufu_procedure.identifier = [identifier.Identifier({"use": "official",
                                                                  "system": self.systems['prod_id'],
                                                                  "value": lufu_procedure_identifier})]
      lufu_procedure.id = id_value

      lufu_procedure_identifier_perf_date = fhirdate.FHIRDate(datetime.strftime(datetime.strptime(self.data['untersuchungsdatum'], '%d.%m.%Y %H:%M:%S'),
                                                                '%Y-%m-%dT%H:%M:%S'))

      lufu_procedure.performedDateTime = lufu_procedure_identifier_perf_date

      ref = {"reference": f"Patient/{self.data['patient_psn']}"}
      lufu_procedure.subject = fhirreference.FHIRReference(jsondict=ref)
      ref = {"reference": f"Encounter/{self.data['encounter_psn']}"}
      lufu_procedure.encounter = fhirreference.FHIRReference(jsondict=ref)

      rep_code_system_cc = codeableconcept.CodeableConcept()
      rep_code_system_cc.coding = [coding.Coding({"system": "https://www.dimdi.de/static/de/klassifikationen/ops",
                     "code": ops_code,
                     "display": ops_text})]

      rep_status = 'completed'

      lufu_procedure.code = rep_code_system_cc
      lufu_procedure.status = rep_status

      return lufu_procedure
    except KeyError as exc:
      self.logger.error(f"In {__name__}: Key {exc} not found in dictionary")
      raise
    except Exception as exc:
      self.logger.error(f"In {__name__}: Error occurred in mapping ({exc})")
      raise


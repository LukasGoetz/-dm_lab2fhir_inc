#!/usr/bin/python3.6

'''Map lufu table data set to FHIR resources of type
   Observation
   Arguments: config
   Returns: FHIR resources
   Author: Jan Scheer, jan.scheer@medma.uni-heidelberg.de
   Date: 06-23-2020'''

from datetime import datetime

import pandas as pd
from lib.mii_profiles import (codeableconcept, coding, fhirdate, fhirreference,
                               identifier, observation, diagnosticreport, period, quantity, meta)

class MapperLuFu2OPS:
  '''Map lufu table data set to FHIR resources of type
   Observation'''

  def __init__(self, logger, systems):
    self.logger = logger
    self.systems = systems
    self.umm_ops_map = dict({
      "1-71" : "Pneumologische Funktionsuntersuchungen",
      "1-715" : "Sechs-Minuten-Gehtest nach Guyatt",
      "1-710" : "Ganzkörperplethysmographie",
      "1-712" : "Spiroergometrie",
      "1-713" : "Messung der funktionellen Residualkapazität [FRC] mit der Helium-Verdünnungsmethode"
    })
    self.umm_loinc_display_map = dict({})

  def codeLookup(self, procedure_code):
    try:
      ops_code_text = 'No Text'
      ops_code_text = self.umm_ops_map[procedure_code]
      return ops_code_text
    except KeyError as exc:
      self.logger.error(f"In {__name__}: Key {exc} not found in dictionary")
      #raise
      return ops_code_text
    except Exception as exc:
      self.logger.error(f"In {__name__}: Error occurred in mapping ({exc})")
      #raise
      return ops_code_text


  def displayLookup(self, loinc_code):
    try:
      loinc_display = 'No Code-Display Value'
      loinc_display = self.umm_loinc_display_map[loinc_code]
      return loinc_display
    except KeyError as exc:
      self.logger.error(f"In {__name__}: Key {exc} not found in dictionary")
      #raise
      return loinc_display
    except Exception as exc:
      self.logger.error(f"In {__name__}: Error occurred in mapping ({exc})")
      #raise
      return loinc_display


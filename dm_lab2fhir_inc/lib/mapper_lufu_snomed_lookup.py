#!/usr/bin/python3.6

'''Map lufu table data set to FHIR resources of type
   Observation
   Arguments: config
   Returns: FHIR resources
   Author: Jan Scheer, jan.scheer@medma.uni-heidelberg.de
   Date: 11-05-2020'''

from datetime import datetime

import pandas as pd
from lib.mii_profiles import (codeableconcept, coding, fhirdate, fhirreference,
                               identifier, observation, diagnosticreport, period, quantity, meta)

class MapperLuFu2Snomed:
  '''Map lufu table data set to FHIR resources of type
   Observation'''

  def __init__(self, logger, systems):
    self.logger = logger
    self.systems = systems
    self.umm_snomed_map = dict({
      "B_PRE": "28275007",
      "B_POST": "28275007",
      "B_": "28275007",
      "SP_PRE": "127783003",
      "SP_POST": "767906009",
      "SP_": "127783003",
      "TRA_": "87529006",
    })
    self.umm_snomed_display_map = dict({
      "28275007": "Total body plethysmography (procedure)",
      "127783003": "Spirometry (procedure)",
      "767906009": "Post bronchodilator spirometry (procedure)",
      "87529006": "Membrane diffusion capacity (procedure)",
    })

  def codeLookup(self, umm_code):
    try:
      sct_code_text = 'No Text'
      sct_code_text = self.umm_snomed_map[umm_code]
      return sct_code_text
    except KeyError as exc:
      self.logger.error(f"In {__name__}: Key {exc} not found in dictionary")
      #raise
      return sct_code_text
    except Exception as exc:
      self.logger.error(f"In {__name__}: Error occurred in mapping ({exc})")
      #raise
      return sct_code_text


  def displayLookup(self, sct_code):
    try:
      sct_code_display = 'No Code-Display Value'
      sct_code_display = self.umm_snomed_display_map[sct_code]
      return sct_code_display
    except KeyError as exc:
      #self.logger.error(f"In {__name__}: Key {exc} not found in dictionary")
      #raise
      return sct_code_display
    except Exception as exc:
      self.logger.error(f"In {__name__}: Error occurred in mapping ({exc})")
      #raise
      return sct_code_display


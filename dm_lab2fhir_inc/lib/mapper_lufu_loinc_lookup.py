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

class MapperLuFu2Loinc:
  '''Map lufu table data set to FHIR resources of type
   Observation'''

  def __init__(self, logger, systems):
    self.logger = logger
    self.systems = systems
    self.umm_loinc_map = dict({
      "SP_PRE_bp_vc_actual" : "82615-6",
      "SP_POST_bpl_vc_actual" : "82616-4",
      "SP_bp_vc_actual": "19866-3",

      "SP_PRE_bp_fvcex_actual" : "19876-2",
      "SP_POST_bpl_fvcex_actual" : "19874-7",
      "SP_bp_fvcex_actual": "19868-9",

      "SP_PRE_bp_fev1_actual" : "20157-4",
      "SP_POST_bpl_fev1_actual" : "20155-8",
      "SP_bp_fev1_actual": "20150-9",

      # bp_fev_vc_actual
      # bp_mef25_actual
      # bp_mef50_actual
      # bp_mef75_actual
      #"SP_PRE_bp_ic_actual" : "LCS-MRCM:pul:ic:prebd",
      #"SP_POST_bpl_ic_actual" : "LCS-MRCM:pul:ic:postbd",
      "SP_bp_ic_actual": "19852-3",

      #"B_PRE_bp_rawtot_actual" : "LCS-MRCM:pul:sreff:prebd",
      #"B_POST_bpl_rawtot_actual" : "LCS-MRCM:pul:sreff:postbd",
      "B_PRE_bp_srawtot_actual": "91980-3",
      "B_bp_srawtot_actual": "91980-3",

      #"B_POST_bpl_srawtot_actual" : "LCS-MRCM:pul:srtot:postbd:prdc",
      # bp_gtot_actual
      # bp_sgtot_actual
      "B_PRE_bp_rv_actual" : "81452-5",
      "B_POST_bpl_rv_actual" : "81453-3",
      "B_bp_rv_actual": "20146-7",

      # bp_rv_tlc_actual
      "B_PRE_bp_tlc_actual" : "81450-9",
      "B_POST_bpl_tlc_actual" : "81451-7",
      "B_bp_tlc_actual": "19859-8",

      # bp_pef_actual
      # bpl_fev1_actual
      # bpl_fev_vcmax_actual
      # bpl_mef25_actual
      # bpl_mef50_actual
      # bpl_mef75_actual
      # bpl_gtot_actual
      # bpl_sgtot_actual
      # bpl_rv_tlc_actual
      # bpl_pef_actual
      # b_r5hz_actual
      # b_x5hz_actual
      # b_fres_actual
      # b_ax_actual
      # bd520actual
      # b_vt_actual
      # bpl_r5hz_actual
      # bpl_x5hz_actual
      # bpl_fres_actual
      # bpl_ax_actual
      # bpld520actual
      # bpl_vt_actual
      # bga_ph_actual
      # bga_pao2_actual
      # bga_paco2_actual
      # bga_be_actual
      # bp_gerstpo2_actual
      # bp_shco3_actual
      # bp_cohb_actual
      # bp_lactat_actual
      "TRA_bp_dlcosb_actual" : "19911-7",
      "TRA_bp_kco_actual" : "19916-6"
      # bp_rvsb_actual
      # vinhetargetwert
      # vinheactualwert
      # bp_hb_actual
      # bp_feno_actual
    })

    self.umm_loinc_display_map = dict({
      "82615-6": "Vital capacity [Volume] Respiratory system by Spirometry --pre bronchodilation",
      "82616-4": "Vital capacity [Volume] Respiratory system by Spirometry --post bronchodilation",
      "19866-3": "Vital capacity [Volume] Respiratory system by Spirometry",
      "19876-2": "Forced vital capacity [Volume] Respiratory system by Spirometry --pre bronchodilation",
      "19874-7": "Forced vital capacity [Volume] Respiratory system by Spirometry --post bronchodilation",
      "20157-4": "FEV1 --pre bronchodilation",
      "20155-8": "FEV1 --post bronchodilation",
      "91980-3": "Specific airway resactualance by Plethysmograph body box",
      "81452-5": "Residual volume --pre bronchodilation",
      "81453-3": "Residual volume --post bronchodilation",
      "81450-9": "Total lung capacity --pre bronchodilation",
      "81451-7": "Total lung capacity --post bronchodilation",
      "19911-7": "Diffusion capacity.carbon monoxide",
      "19916-6": "Diffusion capacity/Alveolar volume",
      "19859-8": "Total lung capacity by Plethysmograph body box",
      "20146-7": "Residual volume",
      "20150-9": "FEV1",
      "19868-9": "Forced vital capacity [Volume] Respiratory system by Spirometry",
      "19852-3": "Inspiratory capacity by Spirometry"
    })

  def codeLookup(self, umm_code):
    try:
      loinc_code = 'No Code'
      loinc_code = self.umm_loinc_map[umm_code]
      return loinc_code
    except KeyError as exc:
      #self.logger.error(f"In {__name__}: Key {exc} not found in dictionary")
      #raise
      return loinc_code
    except Exception as exc:
      self.logger.error(f"In {__name__}: Error occurred in mapping ({exc})")
      #raise
      return loinc_code


  def displayLookup(self, loinc_code):
    try:
      loinc_display = 'No Code-Display Value'
      loinc_display = self.umm_loinc_display_map[loinc_code]
      return loinc_display
    except KeyError as exc:
      #self.logger.error(f"In {__name__}: Key {exc} not found in dictionary")
      #raise
      return loinc_display
    except Exception as exc:
      self.logger.error(f"In {__name__}: Error occurred in mapping ({exc})")
      #raise
      return loinc_display


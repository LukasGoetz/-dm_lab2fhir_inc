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

class MapperLuFu2i2b2:
  '''Map lufu table data set to FHIR resources of type
   Observation'''

  def __init__(self, logger, systems):
    self.logger = logger
    self.systems = systems
    self.umm_i2b2_map = dict({
      "SP_PRE_bp_vc_actual": "LCS-MRCM:pul:vc:prebd",
      "SP_POST_bpl_vc_actual": "LCS-MRCM:pul:vc:postbd",
      "SP_bp_vc_actual": "LCS-MRCM:pul:vc:best",
      "SP_PRE_bp_fvcex_actual" : "LCS-MRCM:pul:fvc:prebd",
      "SP_POST_bpl_fvcex_actual" : "LCS-MRCM:pul:fvc:postbd",
      "SP_bp_fvcex_actual": "LCS-MRCM:pul:fvc:best",
      "SP_PRE_bp_fev1_actual" : "LCS-MRCM:pul:fev1:prebd",
      "SP_POST_bpl_fev1_actual" : "LCS-MRCM:pul:fev1:postbd",
      "SP_bp_fev1_actual": "LCS-MRCM:pul:fev1:best",
      # bp_fev_vc_actual
      # bp_mef25_actual
      # bp_mef50_actual
      # bp_mef75_actual
      "SP_PRE_bp_ic_actual" : "LCS-MRCM:pul:ic:prebd",
      "SP_POST_bpl_ic_actual" : "LCS-MRCM:pul:ic:postbd",
      "SP_bp_ic_actual": "LCS-MRCM:pul:ic:best",
      "B_PRE_bp_rawtot_actual" : "LCS-MRCM:pul:sreff:prebd",
      "B_POST_bpl_rawtot_actual" : "LCS-MRCM:pul:sreff:postbd",
      "B_bp_rawtot_actual": "LCS-MRCM:pul:sreff:best",
      "B_PRE_bp_srawtot_actual" : "LCS-MRCM:pul:srtot:prebd",
      "B_POST_bpl_srawtot_actual" : "LCS-MRCM:pul:srtot:postbd:prdc",
      "B_bp_srawtot_actual": "LCS-MRCM:pul:srtot:best",
      # bp_gtot_actual
      # bp_sgtot_actual
      "B_PRE_bp_rv_actual" : "LCS-MRCM:pul:rv:prebd",
      "B_POST_bpl_rv_actual" : "LCS-MRCM:pul:rv:postbd",
      "B_bp_rv_actual": "LCS-MRCM:pul:rv:best",
      # bp_rv_tlc_actual
      "B_PRE_bp_tlc_actual" : "LCS-MRCM:pul:tlc:prebd",
      "B_POST_bpl_tlc_actual" : "LCS-MRCM:pul:tlc:postbd",
      "B_bp_tlc_actual": "LCS-MRCM:pul:tlc:best",
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
      "TRA_bp_dlcosb_actual": "LCS-MRCM:pul:dlco:best",
      "TRA_bp_kco_actual": "LCS-MRCM:pul:dlcova:best"
      # bp_rvsb_actual
      # vinhetargetwert
      # vinheactualwert
      # bp_hb_actual
      # bp_feno_actual
    })


  def codeLookup(self, umm_code):
    try:
      i2b2_base_code = 'No Code'
      i2b2_base_code = self.umm_i2b2_map[umm_code]
      return i2b2_base_code
    except KeyError as exc:
      #self.logger.error(f"In {__name__}: Key {exc} not found in dictionary")
      #raise
      return i2b2_base_code
    except Exception as exc:
      self.logger.error(f"In {__name__}: Error occurred in mapping ({exc})")
      #raise
      return i2b2_base_code



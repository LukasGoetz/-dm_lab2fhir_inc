#!/usr/bin/python3.6

'''Map lufu table data set to FHIR resources of type
   Observation
   Arguments: config
   Returns: FHIR resources
   Author: Jan Scheer, jan.scheer@medma.uni-heidelberg.de
   Date: 06-23-2020'''
from _sha256 import sha256
from datetime import datetime
from datetime import timezone

import pandas as pd
from lib.mii_profiles import (codeableconcept, coding, fhirdate, fhirreference,
                               identifier, observation, diagnosticreport, period, quantity, meta)
import numpy as np
from lib.mii_profiles.observation import ObservationReferenceRange


class MapperLuFuFall2Obs:
  '''Map lufu table data set to FHIR resources of type
   Observation'''

  def __init__(self, logger, systems, map_table, loinc_mapper, snomed_mapper, i2b2_mapper):
    self.logger = logger
    self.systems = systems
    self.map_table = map_table
    self.data = {}
    self.data_ref_values = []
    self.loinc_mapper = loinc_mapper
    self.snomed_mapper = snomed_mapper
    self.i2b2_mapper = i2b2_mapper

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

    #self.data['untersucher1'] = db_record['untersucher1']
    #self.data['untersucher2'] = db_record['untersucher2']
    self.data['aufenthalt'] = db_record['aufenthalt']
    self.data['versicherungsart'] = db_record['versicherungsart']
    #self.data['geschlecht'] = db_record['geschlecht']
    #self.data['groeße_cm'] = db_record['größe_cm']
    #self.data['gewicht_kg'] = db_record['gewicht_kg']
    self.data['beurteilung'] = db_record['beurteilung']
    self.data['anmerkung'] = db_record['anmerkung']
    self.data['empfehlung'] = db_record['empfehlung']

    self.data['SP_PRE_bp_vc_target'] = db_record['bp_vc_target']
    self.data['SP_PRE_bp_vc_actual'] = db_record['bp_vc_actual']
    self.data['SP_PRE_bp_fvcex_target'] = db_record['bp_fvcex_target']
    self.data['SP_PRE_bp_fvcex_actual'] = db_record['bp_fvcex_actual']
    self.data['SP_PRE_bp_fev1_target'] = db_record['bp_fev1_target']
    self.data['SP_PRE_bp_fev1_actual'] = db_record['bp_fev1_actual']
    self.data['SP_PRE_bp_fev_vc_target'] = db_record['bp_fev_vc_target']
    self.data['SP_PRE_bp_fev_vc_actual'] = db_record['bp_fev_vc_actual']
    self.data['SP_PRE_bp_mef25_target'] = db_record['bp_mef25_target']
    self.data['SP_PRE_bp_mef25_actual'] = db_record['bp_mef25_actual']
    self.data['SP_PRE_bp_mef50_target'] = db_record['bp_mef50_target']
    self.data['SP_PRE_bp_mef50_actual'] = db_record['bp_mef50_actual']
    self.data['SP_PRE_bp_mef75_target'] = db_record['bp_mef75_target']
    self.data['SP_PRE_bp_mef75_actual'] = db_record['bp_mef75_actual']
    self.data['SP_PRE_bp_ic_target'] = db_record['bp_ic_target']
    self.data['SP_PRE_bp_ic_actual'] = db_record['bp_ic_actual']
    self.data['B_PRE_bp_rawtot_target'] = db_record['bp_rawtot_target']
    self.data['B_PRE_bp_rawtot_actual'] = db_record['bp_rawtot_actual']
    self.data['B_PRE_bp_srawtot_target'] = db_record['bp_srawtot_target']
    self.data['B_PRE_bp_srawtot_actual'] = db_record['bp_srawtot_actual']
    self.data['B_PRE_bp_gtot_target'] = db_record['bp_gtot_target']
    self.data['B_PRE_bp_gtot_actual'] = db_record['bp_gtot_actual']
    self.data['B_PRE_bp_sgtot_target'] = db_record['bp_sgtot_target']
    self.data['B_PRE_bp_sgtot_actual'] = db_record['bp_sgtot_actual']
    self.data['B_PRE_bp_rv_target'] = db_record['bp_rv_target']
    self.data['B_PRE_bp_rv_actual'] = db_record['bp_rv_actual']
    self.data['B_PRE_bp_rv_tlc_actual'] = db_record['bp_rv_tlc_actual']
    self.data['B_PRE_bp_rv_tlc_target'] = db_record['bp_rv_tlc_target']
    self.data['B_PRE_bp_tlc_target'] = db_record['bp_tlc_target']
    self.data['B_PRE_bp_tlc_actual'] = db_record['bp_tlc_actual']
    self.data['B_PRE_bp_pef_target'] = db_record['bp_pef_target']
    self.data['B_PRE_bp_pef_actual'] = db_record['bp_pef_actual']

    self.data['SP_POST_bpl_vc_target'] = db_record['bpl_vc_target']
    self.data['SP_POST_bpl_vc_actual'] = db_record['bpl_vc_actual']
    self.data['SP_POST_bpl_fvcex_target'] = db_record['bpl_fvcex_target']
    self.data['SP_POST_bpl_fvcex_actual'] = db_record['bpl_fvcex_actual']
    self.data['SP_POST_bpl_fev1_target'] = db_record['bpl_fev1_target']
    self.data['SP_POST_bpl_fev1_actual'] = db_record['bpl_fev1_actual']
    self.data['SP_POST_bpl_fev_vcmax_target'] = db_record['bpl_fev_vcmax_target']
    self.data['SP_POST_bpl_fev_vcmax_actual'] = db_record['bpl_fev_vcmax_actual']
    self.data['SP_POST_bpl_mef25_target'] = db_record['bpl_mef25_target']
    self.data['SP_POST_bpl_mef25_actual'] = db_record['bpl_mef25_actual']
    self.data['SP_POST_bpl_mef50_target'] = db_record['bpl_mef50_target']
    self.data['SP_POST_bpl_mef50_actual'] = db_record['bpl_mef50_actual']
    self.data['SP_POST_bpl_mef75_target'] = db_record['bpl_mef75_target']
    self.data['SP_POST_bpl_mef75_actual'] = db_record['bpl_mef75_actual']
    self.data['SP_POST_bpl_ic_target'] = db_record['bpl_ic_target2']
    self.data['SP_POST_bpl_ic_actual'] = db_record['bpl_ic_actual']
    self.data['B_POST_bpl_rawtot_target'] = db_record['bpl_rawtot_target']
    self.data['B_POST_bpl_rawtot_actual'] = db_record['bpl_rawtot_actual']
    self.data['B_POST_bpl_srawtot_target'] = db_record['bpl_srawtot_target']
    self.data['B_POST_bpl_srawtot_actual'] = db_record['bpl_srawtot_actual']
    self.data['B_POST_bpl_gtot_target'] = db_record['bpl_gtot_target']
    self.data['B_POST_bpl_gtot_actual'] = db_record['bpl_gtot_actual']
    self.data['B_POST_bpl_sgtot_target'] = db_record['bpl_sgtot_target']
    self.data['B_POST_bpl_sgtot_actual'] = db_record['bpl_sgtot_actual']
    self.data['B_POST_bpl_rv_target'] = db_record['bpl_rv_target']
    self.data['B_POST_bpl_rv_actual'] = db_record['bpl_rv_actual']
    self.data['B_POST_bpl_rv_tlc_target'] = db_record['bpl_rv_tlc_target']
    self.data['B_POST_bpl_rv_tlc_actual'] = db_record['bpl_rv_tlc_actual']
    self.data['B_POST_bpl_tlc_target'] = db_record['bpl_tlc_target']
    self.data['B_POST_bpl_tlc_actual'] = db_record['bpl_tlc_actual']
    self.data['B_POST_bpl_pef_target'] = db_record['bpl_pef_target']
    self.data['B_POST_bpl_pef_actual'] = db_record['bpl_pef_actual']

    self.data['O_PRE_b_r5hz_target'] = db_record['b_r5hz_target']
    self.data['O_PRE_b_r5hz_actual'] = db_record['b_r5hz_actual']
    self.data['O_PRE_b_x5hz_target'] = db_record['b_x5hz_target']
    self.data['O_PRE_b_x5hz_actual'] = db_record['b_x5hz_actual']
    self.data['O_PRE_b_fres_target'] = db_record['b_fres_target']
    self.data['O_PRE_b_fres_actual'] = db_record['b_fres_actual']
    self.data['O_PRE_b_ax_target'] = db_record['b_ax_target']
    self.data['O_PRE_b_ax_actual'] = db_record['b_ax_actual']
    #self.data['O_PRE_bd520actual'] = db_record['bd520actual']
    #self.data['O_PRE_bd520target'] = db_record['bd520target']
    #self.data['O_PRE_b_vt_target'] = db_record['b_vt_target']
    #self.data['O_PRE_b_vt_actual'] = db_record['b_vt_actual']

    self.data['O_POST_bpl_r5hz_target'] = db_record['bpl_r5hz_target']
    self.data['O_POST_bpl_r5hz_actual'] = db_record['bpl_r5hz_actual']
    self.data['O_POST_bpl_x5hz_target'] = db_record['bpl_x5hz_target']
    self.data['O_POST_bpl_x5hz_actual'] = db_record['bpl_x5hz_actual']
    self.data['O_POST_bpl_fres_target'] = db_record['bpl_fres_target']
    self.data['O_POST_bpl_fres_actual'] = db_record['bpl_fres_actual']
    self.data['O_POST_bpl_ax_target'] = db_record['bpl_ax_target']
    self.data['O_POST_bpl_ax_actual'] = db_record['bpl_ax_actual']
    #self.data['O_POST_bpld520target'] = db_record['bpld520target']
    #self.data['O_POST_bpld520actual'] = db_record['bpld520actual']
    #self.data['O_POST_bpl_vt_target'] = db_record['bpl_vt_target']
    #self.data['O_POST_bpl_vt_actual'] = db_record['bpl_vt_actual']

    self.data['BGA_bga_ph_target'] = db_record['bga_ph_target']
    self.data['BGA_bga_ph_actual'] = db_record['bga_ph_actual']
    self.data['BGA_bga_pao2_target'] = db_record['bga_pao2_target']
    self.data['BGA_bga_pao2_actual'] = db_record['bga_pao2_actual']
    self.data['BGA_bga_paco2_target'] = db_record['bga_paco2_target']
    self.data['BGA_bga_paco2_actual'] = db_record['bga_paco2_actual']
    self.data['BGA_bga_be_target'] = db_record['bga_be_target']
    self.data['BGA_bga_be_actual'] = db_record['bga_be_actual']
    self.data['BGA_bp_gerstpo2_target'] = db_record['bp_gerstpo2_target']
    self.data['BGA_bp_gerstpo2_actual'] = db_record['bp_gerstpo2_actual']
    self.data['BGA_bp_shco3_target'] = db_record['bp_shco3_target']
    self.data['BGA_bp_shco3_actual'] = db_record['bp_shco3_actual']
    self.data['BGA_bp_cohb_target'] = db_record['bp_cohb_target']
    self.data['BGA_bp_cohb_actual'] = db_record['bp_cohb_actual']
    self.data['BGA_bp_lactat_target'] = db_record['bp_lactat_target']
    self.data['BGA_bp_lactat_actual'] = db_record['bp_lactat_actual']

    self.data['TRA_bp_dlcosb_target'] = db_record['bp_dlcosb_target']
    self.data['TRA_bp_dlcosb_actual'] = db_record['bp_dlcosb_actual']
    self.data['TRA_bp_kco_target'] = db_record['bp_kco_target']
    self.data['TRA_bp_kco_actual'] = db_record['bp_kco_actual']
    self.data['TRA_bp_rvsb_target'] = db_record['bp_rvsb_target']
    self.data['TRA_bp_rvsb_actual'] = db_record['bp_rvsb_actual']
    #self.data['TRA_vinhetargetwert'] = db_record['vinhetargetwert']
    #self.data['TRA_vinheactualwert'] = db_record['vinheactualwert']
    self.data['TRA_bp_hb_target'] = db_record['bp_hb_target']
    self.data['TRA_bp_hb_actual'] = db_record['bp_hb_actual']

    self.data['FE_bp_feno_target'] = db_record['bp_feno_target']
    self.data['FE_bp_feno_actual'] = db_record['bp_feno_actual']

    #self.data['admission_dt'] = db_record['aufnahmedatum']
    #self.data['discharge_dt'] = db_record['entlassungsdatum']
    concat_elements = []
    record_dict = dict(db_record)
    for key, value in record_dict.items():
        if key == 'quelldatenjahr':
            continue
        concat_elements.append(str(value))
    self.data['concat_elements'] = ''.join(concat_elements)

  def map(self):
    try:
      lufu_observation_list = []
      lufu_observation_B_PRE_list = []
      lufu_observation_B_POST_list = []
      lufu_observation_SP_PRE_list = []
      lufu_observation_SP_POST_list = []
      lufu_observation_BGA_list = []
      lufu_observation_TRA_list = []
      lufu_observation_FE_list = []

      lufu_record_has_b_pre_values = False
      lufu_record_has_b_post_values= False
      lufu_record_has_sp_pre_values = False
      lufu_record_has_sp_post_values = False
      lufu_record_has_bga_values = False
      lufu_record_has_tra_values = False
      lufu_record_has_fe_values = False

      # determine content of lufu record for mapping loinc (pre, post, actual)
      for item in self.data:
        if 'B_PRE' in item:
          if self.data[item] is not None and not np.isnan(self.data[item]):
            lufu_record_has_b_pre_values = True
            lufu_observation_B_PRE_list.append(item)
        if 'B_POST' in item:
          if self.data[item] is not None and not np.isnan(self.data[item]):
            lufu_record_has_b_post_values = True
            lufu_observation_B_POST_list.append(item)
        if 'SP_PRE' in item:
          if self.data[item] is not None and not np.isnan(self.data[item]):
            lufu_record_has_sp_pre_values = True
            lufu_observation_SP_PRE_list.append(item)
        if 'SP_POST' in item:
          if self.data[item] is not None and not np.isnan(self.data[item]):
            lufu_record_has_sp_post_values = True
            lufu_observation_SP_POST_list.append(item)
        if 'BGA_' in item:
          if self.data[item] is not None and not np.isnan(self.data[item]):
            lufu_record_has_bga_values = True
        if 'TRA_' in item:
          if self.data[item] is not None and not np.isnan(self.data[item]):
            lufu_record_has_tra_values = True
            lufu_observation_TRA_list.append(item)
        if 'FE_' in item:
          if self.data[item] is not None and not np.isnan(self.data[item]):
            lufu_record_has_fe_values = True

      if lufu_record_has_b_pre_values and not lufu_record_has_b_post_values:
        lufu_observation_list.append(mapLufuItems2Obs(self, 'B_', lufu_observation_B_PRE_list))
      if lufu_record_has_b_pre_values and lufu_record_has_b_post_values:
        lufu_observation_list.append(mapLufuItems2Obs(self, 'B_PRE', lufu_observation_B_PRE_list))
      if lufu_record_has_b_post_values:
        lufu_observation_list.append(mapLufuItems2Obs(self, 'B_POST', lufu_observation_B_POST_list))
      if lufu_record_has_sp_pre_values and not lufu_record_has_sp_post_values:
        lufu_observation_list.append(mapLufuItems2Obs(self, 'SP_', lufu_observation_SP_PRE_list))
      if lufu_record_has_sp_pre_values and lufu_record_has_sp_post_values:
        lufu_observation_list.append(mapLufuItems2Obs(self, 'SP_PRE', lufu_observation_SP_PRE_list))
      if lufu_record_has_sp_post_values:
        lufu_observation_list.append(mapLufuItems2Obs(self, 'SP_POST', lufu_observation_SP_POST_list))
      if lufu_record_has_tra_values:
        lufu_observation_list.append(mapLufuItems2Obs(self, 'TRA_', lufu_observation_TRA_list))

      lufu_observation_list = [x for x in lufu_observation_list if x != []]
      return lufu_observation_list
    except KeyError as exc:
      self.logger.error(f"In {__name__}: Key {exc} not found in dictionary")
      raise
    except Exception as exc:
      self.logger.error(f"In {__name__}: Error occurred in mapping ({exc})")
      raise

def mapLufuItems2Obs(self, param, items):

    sct_code = self.snomed_mapper.codeLookup(str(param))
    sct_display = self.snomed_mapper.displayLookup(str(sct_code))

    lufu_observation = observation.Observation()
    observation_unit = ''
    concat_string = self.data['concat_elements'] + param
    id_value = sha256(concat_string.encode('utf-8')).hexdigest()
    lufu_observation.id = id_value
    lufu_observation.identifier = [identifier.Identifier({"system": self.systems['lufu_obs_id'],
                                                          "value": id_value})]
    obs_actual_code = codeableconcept.CodeableConcept()
    obs_actual_code.coding = [coding.Coding({"system": "http://snomed.info/sct",
                                             "code": sct_code,
                                             "display": sct_display,
                                             "version": "0.1"})]
    # obs category
    obs_type_sys = "http://terminology.hl7.org/CodeSystem/observation-category"
    code = 'exam'
    observation_cat = codeableconcept.CodeableConcept()
    observation_cat.coding = [coding.Coding({"system": obs_type_sys,
                                             "code": code})]
    lufu_observation.category = [observation_cat]
    lufu_observation.code = obs_actual_code

    ref = {"reference": f"Encounter/{self.data['encounter_psn']}"}
    lufu_observation.encounter = fhirreference.FHIRReference(jsondict=ref)
    ref = {"reference": f"Patient/{self.data['patient_psn']}"}
    lufu_observation.subject = fhirreference.FHIRReference(jsondict=ref)

    if not pd.isna(self.data['untersuchungsdatum']) and not pd.isna(self.data['untersuchungsuhrzeit']):
        dt_string = self.data['untersuchungsdatum'] + " " + self.data['untersuchungsuhrzeit']
        obs_datetime = datetime.strptime(dt_string, "%Y-%m-%d %H:%M:%S")
        obs_datetime.replace(tzinfo=timezone.utc)
        # collect_ts_fhir = datetime.strftime(obs_datetime.astimezone().isoformat(), '%Y-%m-%dT%H:%M:%S')
        lufu_observation.effectiveDateTime = fhirdate.FHIRDate(obs_datetime.astimezone().isoformat())

    components = []

    for item in items:
        if 'actual' in str(item):
            obs_component = observation.ObservationComponent()
            lufu_value = self.data[item]
            lookupParam = item
            if ("PRE" not in param) and ("POST" not in param):
                lookupParam = str(item).replace("PRE_", "")
            loinc_code = self.loinc_mapper.codeLookup(lookupParam)
            loinc_display = self.loinc_mapper.displayLookup(loinc_code)
            i2b2_code = self.i2b2_mapper.codeLookup(lookupParam)

            codings = []

            if loinc_code != 'No Code':
                codings.append(
                    coding.Coding({"system": "http://loinc.org",
                                   "code": loinc_code,
                                   "display": loinc_display,
                                   "version": "2.46"})
                )
                mask = self.map_table[self.map_table['RELMA'] == loinc_code]
                observation_unit = mask['Unit'].values[0]
            if i2b2_code != 'No Code':
                codings.append(
                    coding.Coding({"system": "http://mdr.miracum.org",
                                   "code": i2b2_code,
                                   "version": "0.01"})
                )
                mask = self.map_table[self.map_table['I2B2 Basecode'] == i2b2_code]
                observation_unit = mask['Unit'].values[0]

            if len(codings) > 0:
                obs_component_code = codeableconcept.CodeableConcept()
                obs_component_code.coding = codings
                obs_component.code = obs_component_code
                #obs_component.valueDateTime = fhirdate.FHIRDate(collect_ts_fhir)
                obs_component.valueQuantity = quantity.Quantity({"value": lufu_value,
                                                                     "unit": observation_unit,
                                                                     "system": "http://unitsofmeasure.org",
                                                                     "code": observation_unit})
                lufu_ref_value = self.data[str(item).replace('actual', 'target')]
                low_ref_value = quantity.Quantity({"value": lufu_ref_value,
                                                   "unit": observation_unit,
                                                   "system": "http://unitsofmeasure.org",
                                                   "code": observation_unit})
                ref_range = ObservationReferenceRange()
                ref_range.low = low_ref_value
                ref_range.high = low_ref_value
                obs_component.referenceRange = [ref_range]

                components.append(obs_component)

    lufu_observation.component = components
    lufu_observation.status = "final"

    lufu_actual_meta = meta.Meta()
    lufu_actual_meta.source = "#lufu-cwd"
    lufu_observation.meta = lufu_actual_meta
    if len(lufu_observation.component) > 0:
        return lufu_observation
    else:
        return []


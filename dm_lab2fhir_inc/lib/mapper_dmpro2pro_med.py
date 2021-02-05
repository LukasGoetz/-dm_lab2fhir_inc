#!/usr/bin/python3.6

'''Map procedure table of datamart to FHIR resources of type
   Procedure, Medication, MedicationStatement
   Arguments: config
   Returns: FHIR resources
   Author: Lukas Goetz, Lukas.Goetz@medma.uni-heidelberg.de
   Date: 04-19-2020'''

import pandas as pd
from datetime import datetime
from hashlib import sha256
from lib.mii_profiles import (mii_codeableconcept, mii_coding, dosage, fhirdate, fhirreference,
                              identifier, medication, medicationstatement, period, codeableconcept, coding,
                              mii_procedure, quantity, range, meta)

class MapperDMPro2ProMed:
  '''Map procedure table of datamart to FHIR resources of type
     Procedure, Medication, MedicationStatement'''

  def __init__(self, logger, systems, map_table):
    self.logger = logger
    self.systems = systems
    self.ops_drug_mapping = map_table[0]
    self.drug_unii_mapping = map_table[1]
    self.data = {}

  def read(self, encounter_psn, patient_psn, admission_dt, discharge_dt, db_record):
    self.data['encounter_psn'] = encounter_psn
    self.data['procedure_nr'] = db_record.procedure_nr
    self.data['patient_psn'] = patient_psn
    self.data['admission_dt'] = admission_dt
    self.data['discharge_dt'] = discharge_dt
    self.data['ops_kode'] = db_record.ops_code
    self.data['lokalisation'] = db_record.procedure_laterality
    self.data['ops_datum'] = db_record.procedure_begin_timestamp
    self.data['ops_version'] = db_record.ops_id

    #concat_elements = []
    #concat_elements.append(encounter_psn)
    #concat_elements.append(patient_psn)
    #concat_elements.append(str(admission_dt))
    #concat_elements.append(str(discharge_dt))

    #record_dict = dict(db_record._asdict())
    #for key, value in record_dict.items():
    #  concat_elements.append(str(value))
    #self.data['concat_elements'] = ''.join(concat_elements)

  def _map_dmpro2medi(self, encounter_ref, patient_ref):
    new_medication = []
    medication_stm = []
    mapping_row = self.ops_drug_mapping[self.ops_drug_mapping['ops_code'] == self.data['ops_kode']]
    if not mapping_row.empty:
      drug_name = mapping_row['medication'].to_string(index=False)[1:]
      drug_atc = mapping_row['atc_code1'].to_string(index=False)[1:]
      dosage_text = mapping_row['text'].to_string(index=False)[1:]
      ucum_short = mapping_row['ucum_short'].to_string(index=False)[1:]
      ucum_full = mapping_row['ucum_full'].to_string(index=False)[1:]
      dosage_min = mapping_row['dosage_min'].to_string(index=False)[1:]
      dosage_max = mapping_row['dosage_max'].to_string(index=False)[1:]
      combi_product = mapping_row['medication_combi'].to_string(index=False)

      # generate medication statement resources
      new_medication = medication.Medication()
      id_value = str(self.data['encounter_psn']) + '_' + str(self.data['procedure_nr']) + '_med'
      #id_value = f"{drug_name}"
      #id_value = sha256(id_value.encode('utf-8')).hexdigest()
      new_medication.id = id_value
      new_medication.identifier = [identifier.Identifier({"system": self.systems['med_id'],
                                                          "value": id_value})]

      atc_cc = codeableconcept.CodeableConcept()
      atc_cc.coding = [coding.Coding({"system": "http://fhir.de/CodeSystem/dimdi/atc",
                                      "code": drug_atc,
                                      "version": "ATC/DDD Version 2020",
                                      "display": drug_name})]
      new_medication.code = atc_cc
      new_medication.status = "active"
      med_meta = meta.Meta()
      med_meta.source = "#sap-ish"
      new_medication.meta = med_meta

      substances = [drug_name]
      if combi_product:
        substances = drug_name.split('-')

      ingredients = []
      if substances != ["UNKLAR"]:
        for substance in substances:
          mapping_row2 = self.drug_unii_mapping[self.drug_unii_mapping['Substanzangabe_aus_OPS-Code'] == substance]
          substance_unii = mapping_row2['Substanz_fuer_Dosisberechnung_UNII-number'].to_string(index=False)[1:]
          substance_ask = mapping_row2['Substanz_fuer_Dosisberechnung_ASK-Nr'].to_string(index=False)[1:]
          substance_cas = mapping_row2['Substanz_fuer_Dosisberechnung_CAS-Nummer'].to_string(index=False)[1:]

          ingredient = medication.MedicationIngredient()
          ingredient_cc = codeableconcept.CodeableConcept()
          ingredient_cc.coding = [coding.Coding({"system": "http://fdasis.nlm.nih.gov",
                                                 "code": substance_unii,
                                                 "display": substance})]
          ingredient_cc.coding += [coding.Coding({"system": "http://fhir.de/CodeSystem/ask",
                                                  "code": substance_ask,
                                                  "display": substance})]
          ingredient_cc.coding += [coding.Coding({"system": "urn:oid:2.16.840.1.113883.6.61",
                                                  "code": substance_cas,
                                                  "display": substance})]
          ingredient.itemCodeableConcept = ingredient_cc
          ingredients.append(ingredient)
      new_medication.ingredient = ingredients

      # generate medication statement resources
      id_value = str(self.data['encounter_psn']) + '_' + str(self.data['procedure_nr']) + '_med_stat'
      #id_value = f"{self.data['concat_elements']}-medication"
      #id_value = sha256(id_value.encode('utf-8')).hexdigest()

      medication_stm = medicationstatement.MedicationStatement()
      medication_stm.id = id_value
      medication_stm.identifier = [identifier.Identifier({"system": self.systems['med_stm_id'],
                                                          "value": id_value})]
      med_meta = meta.Meta()
      med_meta.source = "#sap-ish"
      medication_stm.meta = med_meta      

      ref = {"reference": f"Medication/{new_medication.id}"}
      medication_ref = fhirreference.FHIRReference(jsondict=ref)
      medication_stm.medicationReference = medication_ref
      medication_stm.context = encounter_ref
      medication_stm.subject = patient_ref
      #medication_stm.partOf =

      medication_stm.status = "active"

      if not pd.isna(self.data['admission_dt']):
        med_period = period.Period()
        med_period.start = fhirdate.FHIRDate(datetime.strftime(self.data['admission_dt'],
                                                               '%Y-%m-%dT%H:%M:%S'))
        if not pd.isna(self.data['discharge_dt']):
          med_period.end = fhirdate.FHIRDate(datetime.strftime(self.data['discharge_dt'],
                                                               '%Y-%m-%dT%H:%M:%S'))
          medication_stm.status = "completed"
        medication_stm.effectivePeriod = med_period

      new_dosage = dosage.Dosage()
      new_dosage.text = dosage_text

      dose_range = range.Range()
      if dosage_min != "NaN":
        dose_range.low = quantity.Quantity({"value": float(dosage_min.replace(',', '.')),
                                            "unit": ucum_full,
                                            "system": "http://unitsofmeasure.org",
                                            "code": ucum_short})
      if dosage_max != "NaN":
        dose_range.high = quantity.Quantity({"value": float(dosage_max.replace(',', '.')),
                                             "unit": ucum_full,
                                             "system": "http://unitsofmeasure.org",
                                             "code": ucum_short})
      dose_rate = dosage.DosageDoseAndRate()
      dose_rate.doseRange = dose_range
      new_dosage.doseAndRate = [dose_rate]

      #method, route, site

      medication_stm.dosage = [new_dosage]
    return [new_medication, medication_stm]

  def map(self):
    try:
      new_medication = []
      medication_stm = []

      #id_value = sha256(self.data['concat_elements'].encode('utf-8')).hexdigest()
      id_value = str(self.data['encounter_psn']) + '_' + str(self.data['procedure_nr'])

      pat_procedure = mii_procedure.Procedure()
      pat_procedure.id = id_value
      pat_procedure.identifier = [identifier.Identifier({"system": self.systems['prod_id'],
                                                         "value": id_value})]
      pat_procedure.status = "completed"

      ref = {"reference": f"Encounter/{self.data['encounter_psn']}"}
      encounter_ref = fhirreference.FHIRReference(jsondict=ref)
      ref = {"reference": f"Patient/{self.data['patient_psn']}"}
      patient_ref = fhirreference.FHIRReference(jsondict=ref)

      pat_procedure.encounter = encounter_ref
      pat_procedure.subject = patient_ref

      category = mii_codeableconcept.CodeableConcept()
      category.coding = [mii_coding.Coding({"system":"http://snomed.info/sct",
                                            "code":"387713003"})]
      pat_procedure.category = category

      procedure_code = mii_codeableconcept.CodeableConcept()
      procedure_code.coding = [mii_coding.Coding({"system": "http://fhir.de/CodeSystem/dimdi/ops",
                                                  "code": self.data['ops_kode'],
                                                  "version": self.data['ops_version']})]
      pat_procedure.code = procedure_code

      if self.data['ops_kode'][0] == '6':
        [new_medication, medication_stm] = self._map_dmpro2medi(encounter_ref, patient_ref)

      if not pd.isna(self.data['ops_datum']):
        ops_datum_fhir = datetime.strftime(self.data['ops_datum'], '%Y-%m-%dT%H:%M:%S')
        pat_procedure.performedDateTime = fhirdate.FHIRDate(ops_datum_fhir)

      if self.data['lokalisation']:
        pat_procedure.bodySite = []
        snomed_ver = "http://snomed.info/sct/900000000000207008/version/20200309"
        if self.data['lokalisation'] == 'L' or self.data['lokalisation'] == 'B':
          left_body_part = mii_codeableconcept.CodeableConcept()
          left_body_part.coding = [mii_coding.Coding({"system": "http://snomed.info/sct",
                                                  "code": "31156008",
                                                  "display": '''Structure of left half
                                                                of body (body structure)''',
                                                  "version": snomed_ver})]
          pat_procedure.bodySite.append(left_body_part)

        if self.data['lokalisation'] == 'R' or self.data['lokalisation'] == 'B':
          right_body_part = mii_codeableconcept.CodeableConcept()
          right_body_part.coding = [mii_coding.Coding({"system": "http://snomed.info/sct",
                                                   "code": "85421007",
                                                   "display": '''Structure of right half
                                                                 of body (body structure)''',
                                                   "version": snomed_ver})]
          pat_procedure.bodySite.append(right_body_part)

      pro_meta = meta.Meta()
      pro_meta.source = "#sap-ish"
      pat_procedure.meta = pro_meta

      return [pat_procedure, new_medication, medication_stm]
    except KeyError as exc:
      self.logger.error(f"In {__name__}: Key {exc} not found in dictionary")
      raise
    #except Exception as exc:
    #  self.logger.error(f"In {__name__}: Error occurred in mapping ({exc})")
    #  raise

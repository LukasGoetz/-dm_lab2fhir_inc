#!/usr/bin/python3.6

import requests
import re

class Pseudonymizer:
  '''Pseudomyize patient and encounter ID
   Arguments: logger, psn_url'''  

  def __init__(self, logger, psn_url):
    self.logger = logger
    self.psn_url = psn_url

  # Pseudomyize patient_id by gPAS
  def request_patient_psn(self, patient_id):
    try:
      response = ''
      headers = {"Content-Type": "application/xml;charset=utf-8"}
      body = f'''<soapenv:Envelope xmlns:soapenv=\"http://schemas.xmlsoap.org/soap/envelope/\"
                 xmlns:psn=\"http://psn.ttp.ganimed.icmvc.emau.org/\">
                   <soapenv:Header/>
                   <soapenv:Body>
                     <psn:getOrCreatePseudonymFor>
                       <value>{patient_id}</value>
                       <domainName>patient_id</domainName>
                     </psn:getOrCreatePseudonymFor>
                   </soapenv:Body>
                  </soapenv:Envelope>'''

      session = requests.Session()
      session.trust_env = False
      response = session.post(self.psn_url, headers=headers, data=body)
      response.raise_for_status()

      match = re.search(r"<psn>(.*)<\/psn>", str(response.content))
      patient_psn = match[1]

      return patient_psn
    except Exception as exc:
      self.logger.error(f"In {__name__}: Pseudonym could not be created ({exc})")
      self.logger.error(f"In {__name__}: Response: {response.json()}")
      raise

  # Pseudomyize encounter_id (case_id) by gPAS
  def request_encounter_psn(self, encounter_id):
    try:
      response = ''
      headers = {"Content-Type": "application/xml;charset=utf-8"}
      body = f'''<soapenv:Envelope xmlns:soapenv=\"http://schemas.xmlsoap.org/soap/envelope/\"
                xmlns:psn=\"http://psn.ttp.ganimed.icmvc.emau.org/\">
                  <soapenv:Header/>
                  <soapenv:Body>
                    <psn:getOrCreatePseudonymFor>
                      <value>{encounter_id}</value>
                      <domainName>encounter_id</domainName>
                    </psn:getOrCreatePseudonymFor>
                </soapenv:Body>
              </soapenv:Envelope>'''

      session = requests.Session()
      session.trust_env = False
      response = session.post(self.psn_url, headers=headers, data=body)
      response.raise_for_status()

      match = re.search(r"<psn>(.*)<\/psn>", str(response.content))
      encounter_psn = match[1]

      return encounter_psn
    except Exception as exc:
      self.logger.error(f"In {__name__}: Pseudonym could not be created ({exc})")
      self.logger.error(f"In {__name__}: Response: {response.json()}")
      raise
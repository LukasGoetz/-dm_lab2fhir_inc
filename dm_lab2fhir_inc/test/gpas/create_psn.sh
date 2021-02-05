#!/bin/bash
#create pseudonym for patient id
curl -X POST -H "Content-Type:application/xml" --data @patient_id http://localhost:8080/gpas/DomainService
#create pseudonym for encounter id
curl -X POST -H "Content-Type:application/xml" --data @encounter_id http://localhost:8080/gpas/DomainService

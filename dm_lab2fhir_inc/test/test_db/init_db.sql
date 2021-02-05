--create user and schemas
CREATE USER dwh WITH ENCRYPTED PASSWORD 'dwh';
CREATE SCHEMA dwh AUTHORIZATION dwh;
CREATE USER stg_sap WITH ENCRYPTED PASSWORD 'stg_sap';
CREATE SCHEMA stg_sap AUTHORIZATION stg_sap;
CREATE USER stg_fhir_dm WITH ENCRYPTED PASSWORD 'stg_fhir_dm';
CREATE SCHEMA stg_fhir_dm AUTHORIZATION stg_fhir_dm;

ALTER DATABASE postgres SET timezone TO 'Europe/Berlin';

--create tables for dwh schema
CREATE TABLE dwh.rf_med_cov_patient (
    patient_id integer,
    patient_lastname character varying,
    patient_firstname character varying,
    patient_birthdate date,
    patient_sex character varying,
    patient_address_countrycode character varying,
    patient_address_zipcode character varying,
    patient_address_city character varying,
    patient_address_street character varying,
    patient_address_admregioncode character varying,
    patient_insurance_identifier character varying,
    patient_deceased_flag integer,
    patient_deceased_timestamp timestamp with time zone,
    patient_last_update date
);
CREATE TABLE dwh.rf_med_cov_encounter (
    encounter_id bigint,
    patient_id integer,
    encountertype_id integer,
    encounter_organization_id integer,
    encounter_admregioncode character varying(9),
    encounter_foreigner_flag integer,
    admission_age_years double precision,
    admission_age_days double precision,
    admission_timestamp timestamp with time zone,
    admission_unit_id character varying,
    admission_event_id character varying,
    admission_drg_reason_id character varying,
    admission_event_reason character varying,
    discharge_flag integer,
    discharge_timestamp timestamp with time zone,
    discharge_unit_id character varying,
    discharge_event_id character varying,
    discharge_event_reason character varying,
    bodyweight numeric,
    bodyweight_units character varying,
    ventilation_hours integer,
    icu_days double precision,
    intercurrent_dialyses bigint,
    encounter_last_update date
);
CREATE TABLE dwh.rf_med_cov_diagnosis (
    encounter_id integer,
    patient_id integer,
    diagnosis_nr integer,
    diagnosis_event_nr integer,
    diagnosis_documentation_timestamp timestamp with time zone,
    icd_id character varying,
    icd_code character varying,
    supplementary_icd_id character varying,
    supplementary_icd_code character varying,
    diagnosis_laterality character varying,
    diagnosis_verificationstatus character varying,
    diagnosis_text character varying,
    principal_diagnosis_flag integer,
    admission_diagnosis_flag integer,
    discharge_diagnosis_flag integer
);
CREATE TABLE dwh.rf_med_cov_procedure (
    encounter_id bigint,
    patient_id integer,
    procedure_nr integer,
    procedure_event_nr integer,
    procedure_unit_id character varying,
    procedure_begin_timestamp timestamp with time zone,
    procedure_end_timestamp timestamp with time zone,
    ops_id character varying,
    ops_code character varying(10),
    procedure_laterality character varying,
    procedure_text character varying(50),
    principal_procedure_flag integer
);
CREATE TABLE dwh.rf_med_cov_transfer (
    encounter_id integer,
    patient_id integer,
    event_nr integer,
    event_nr_previous integer,
    event_nr_next integer,
    event_id character varying,
    event_planned_flag integer,
    event_ongoing_flag integer,
    event_unit_id character varying,
    event_room_id character varying,
    event_room_name character varying,
    event_bed_id character varying,
    event_bed_name character varying,
    event_bed_shortname character varying,
    event_begin_timestamp timestamp with time zone,
    event_end_timestamp timestamp with time zone,
    event_duration_hours double precision,
    event_unit_id_previous character varying,
    event_unit_id_next character varying
);
CREATE TABLE dwh.rd_med_cov_unit (
    organization_id integer,
    organization_name character varying,
    organization_code integer,
    dept_id character varying,
    dept_name character varying,
    dept_code character varying,
    dept_p301_code character varying,
    dept_shortname character varying,
    dept_valid_from timestamp without time zone,
    dept_valid_until timestamp without time zone,
    dept_valid_flag integer,
    unit_id character varying,
    unit_name character varying,
    unit_code character varying,
    unit_shortname character varying,
    unit_valid_from timestamp without time zone,
    unit_valid_until timestamp without time zone,
    unit_valid_flag integer,
    unit_type_id character varying,
    unit_type_name character varying,
    unit_interdisciplinary_flag integer,
    unit_icu_flag integer
);
CREATE TABLE dwh.f_med_lab_result (
    result_id bigint,
    specimen_id bigint,
    order_id integer,
    encounter_id integer,
    patient_id integer,
    material_id integer,
    method_id integer,
    loinc_code character varying,
    analysis_device_id integer,
    collection_timestamp timestamp with time zone,
    arrival_timestamp timestamp with time zone,
    result_timestamp timestamp with time zone,
    result_value character varying,
    result_value_type character varying,
    result_value_comparator character varying,
    result_value_num double precision,
    result_unit character varying,
    result_note character varying,
    result_interpretation_flag character varying,
    result_reference_range character varying,
    result_reference_low double precision,
    result_reference_high double precision
);

--create tables for stg_sap schema
CREATE TABLE stg_sap.q_npat (
    stdat date,
    patnr character varying
);
CREATE TABLE stg_sap.q_nfal (
    stdat date,
    falnr character varying
);
CREATE TABLE stg_sap.q_nbew (
    stdat date,
    erdat date,
    updat date,
    lfdnr character varying,
    falnr character varying
);
CREATE TABLE stg_sap.q_ndia (
    stdat date,
    erdat date,
    updat date,
    lfdnr character varying,
    falnr character varying
);
CREATE TABLE stg_sap.q_nicp (
    stdat date,
    updat date,
    icpml character varying,
    lnric character varying,
    falnr character varying
);

--create table for stg_fhir_dm schema
CREATE TABLE stg_fhir_dm.resources_inc
(
    id              SERIAL,
    fhir_id         varchar(64) NOT NULL,
    type            varchar(64) NOT NULL,
    data            jsonb       NOT NULL,
    created_at      timestamp   NOT NULL DEFAULT NOW(),
    last_updated_at timestamp   NOT NULL DEFAULT NOW(),
    is_deleted      boolean     NOT NULL DEFAULT FALSE,
    CONSTRAINT fhir_id_unique UNIQUE (fhir_id, type)
);
ALTER TABLE dwh.rf_med_cov_patient OWNER TO dwh;
ALTER TABLE dwh.rf_med_cov_encounter OWNER TO dwh;
ALTER TABLE dwh.rf_med_cov_diagnosis OWNER TO dwh;
ALTER TABLE dwh.rf_med_cov_procedure OWNER TO dwh;
ALTER TABLE dwh.rf_med_cov_transfer OWNER TO dwh;
ALTER TABLE dwh.rd_med_cov_unit OWNER TO dwh;
ALTER TABLE dwh.f_med_lab_result OWNER TO dwh;
ALTER TABLE stg_sap.q_npat OWNER TO stg_sap;
ALTER TABLE stg_sap.q_nfal OWNER TO stg_sap;
ALTER TABLE stg_sap.q_ndia OWNER TO stg_sap;
ALTER TABLE stg_sap.q_nicp OWNER TO stg_sap;
ALTER TABLE stg_sap.q_nbew OWNER TO stg_sap;
ALTER TABLE stg_fhir_dm.resources_inc OWNER TO stg_fhir_dm;

--fill tables of dwh schema
COPY dwh.rf_med_cov_patient FROM '/srv/test_data/rf_med_cov_patient.csv' CSV HEADER;
COPY dwh.rf_med_cov_encounter FROM '/srv/test_data/rf_med_cov_encounter.csv' CSV HEADER;
COPY dwh.rf_med_cov_diagnosis FROM '/srv/test_data/rf_med_cov_diagnosis.csv' CSV HEADER;
COPY dwh.rf_med_cov_procedure FROM '/srv/test_data/rf_med_cov_procedure.csv' CSV HEADER;
COPY dwh.rf_med_cov_transfer FROM '/srv/test_data/rf_med_cov_transfer.csv' CSV HEADER;
COPY dwh.rd_med_cov_unit FROM '/srv/test_data/rd_med_cov_unit.csv' CSV HEADER;
COPY dwh.f_med_lab_result FROM '/srv/test_data/f_med_lab_result.csv' CSV HEADER;

--fill tables of stg_sap schema
COPY stg_sap.q_npat FROM '/srv/test_data/q_npat.csv' CSV HEADER;
COPY stg_sap.q_nfal FROM '/srv/test_data/q_nfal.csv' CSV HEADER;
COPY stg_sap.q_nbew FROM '/srv/test_data/q_nbew.csv' CSV HEADER;
COPY stg_sap.q_ndia FROM '/srv/test_data/q_ndia.csv' CSV HEADER;
COPY stg_sap.q_nicp FROM '/srv/test_data/q_nicp.csv' CSV HEADER;

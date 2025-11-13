-- eFiche Clinical Data Schema
-- This schema stores patient encounters, procedures, diagnoses, and radiology reports
-- Designed to handle scalability and future support for embeddings

-- Create extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Patients table: Core patient demographics
CREATE TABLE IF NOT EXISTS patients (
    patient_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    age INTEGER CHECK (age >= 0 AND age <= 120),
    sex VARCHAR(10) CHECK (sex IN ('Male', 'Female', 'Other', 'Unknown')),
    location VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Index on commonly queried fields
CREATE INDEX idx_patients_age ON patients(age);
CREATE INDEX idx_patients_sex ON patients(sex);

-- Facilities table: Hospital or clinic information
CREATE TABLE IF NOT EXISTS facilities (
    facility_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    facility_name VARCHAR(255) NOT NULL,
    facility_type VARCHAR(50),
    location VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Encounters table: Patient visits or admissions
CREATE TABLE IF NOT EXISTS encounters (
    encounter_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    patient_id UUID NOT NULL REFERENCES patients(patient_id) ON DELETE CASCADE,
    facility_id UUID REFERENCES facilities(facility_id),
    encounter_date DATE NOT NULL,
    encounter_type VARCHAR(50),
    status VARCHAR(20) DEFAULT 'completed',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for faster joins and queries
CREATE INDEX idx_encounters_patient ON encounters(patient_id);
CREATE INDEX idx_encounters_date ON encounters(encounter_date);
CREATE INDEX idx_encounters_facility ON encounters(facility_id);

-- Procedures table: Medical procedures performed during encounters
CREATE TABLE IF NOT EXISTS procedures (
    procedure_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    encounter_id UUID NOT NULL REFERENCES encounters(encounter_id) ON DELETE CASCADE,
    procedure_code VARCHAR(50),
    procedure_name VARCHAR(255),
    modality VARCHAR(50), -- e.g., X-ray, CT, MRI
    projection VARCHAR(50), -- e.g., PA, AP, lateral
    procedure_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_procedures_encounter ON procedures(encounter_id);
CREATE INDEX idx_procedures_modality ON procedures(modality);

-- Diagnosis codes reference table
CREATE TABLE IF NOT EXISTS diagnosis_codes (
    code_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    code VARCHAR(50) UNIQUE NOT NULL,
    description TEXT,
    code_system VARCHAR(50) DEFAULT 'ICD-10', -- ICD-10, SNOMED, etc.
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_diagnosis_codes_code ON diagnosis_codes(code);

-- Diagnoses table: Links encounters to diagnosis codes
CREATE TABLE IF NOT EXISTS diagnoses (
    diagnosis_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    encounter_id UUID NOT NULL REFERENCES encounters(encounter_id) ON DELETE CASCADE,
    code_id UUID REFERENCES diagnosis_codes(code_id),
    diagnosis_date DATE,
    is_primary BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_diagnoses_encounter ON diagnoses(encounter_id);
CREATE INDEX idx_diagnoses_code ON diagnoses(code_id);

-- Reports table: Clinical reports (radiology, pathology, etc.)
CREATE TABLE IF NOT EXISTS reports (
    report_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    encounter_id UUID NOT NULL REFERENCES encounters(encounter_id) ON DELETE CASCADE,
    report_type VARCHAR(50) DEFAULT 'radiology',
    report_text TEXT,
    language VARCHAR(10) DEFAULT 'en',
    -- Future support for embeddings
    text_embedding VECTOR(768), -- Will be populated later with ML models
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_reports_encounter ON reports(encounter_id);
CREATE INDEX idx_reports_type ON reports(report_type);

-- PadChest staging table: Stores raw data from external source before processing
CREATE TABLE IF NOT EXISTS padchest_staging (
    staging_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    image_id VARCHAR(255) UNIQUE NOT NULL,
    patient_age INTEGER,
    patient_sex VARCHAR(10),
    study_date DATE,
    projection VARCHAR(50),
    modality VARCHAR(50),
    labels TEXT, -- Comma-separated diagnosis labels
    report_text TEXT,
    processed BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_padchest_processed ON padchest_staging(processed);
CREATE INDEX idx_padchest_image_id ON padchest_staging(image_id);

-- Audit log for tracking data changes
CREATE TABLE IF NOT EXISTS audit_log (
    log_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    table_name VARCHAR(50),
    operation VARCHAR(10),
    record_id UUID,
    changed_by VARCHAR(100) DEFAULT 'system',
    changed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    old_values JSONB,
    new_values JSONB
);

CREATE INDEX idx_audit_log_table ON audit_log(table_name);
CREATE INDEX idx_audit_log_timestamp ON audit_log(changed_at);
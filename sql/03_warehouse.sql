-- Data Warehouse Schema for Analytics
-- Star Schema with Dimension and Fact Tables

-- Dimension: Patient
CREATE TABLE IF NOT EXISTS dim_patient (
    patient_key SERIAL PRIMARY KEY,
    patient_id UUID UNIQUE NOT NULL,
    age INTEGER,
    sex VARCHAR(10),
    location VARCHAR(100),
    age_group VARCHAR(20),  -- Derived: '18-30', '31-50', '51-70', '71+'
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_dim_patient_id ON dim_patient(patient_id);
CREATE INDEX idx_dim_patient_age_group ON dim_patient(age_group);

-- Dimension: Facility
CREATE TABLE IF NOT EXISTS dim_facility (
    facility_key SERIAL PRIMARY KEY,
    facility_id UUID UNIQUE NOT NULL,
    facility_name VARCHAR(255),
    facility_type VARCHAR(50),
    location VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_dim_facility_id ON dim_facility(facility_id);

-- Dimension: Procedure
CREATE TABLE IF NOT EXISTS dim_procedure (
    procedure_key SERIAL PRIMARY KEY,
    procedure_id UUID UNIQUE NOT NULL,
    procedure_name VARCHAR(255),
    modality VARCHAR(50),
    projection VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_dim_procedure_id ON dim_procedure(procedure_id);
CREATE INDEX idx_dim_procedure_modality ON dim_procedure(modality);

-- Dimension: Diagnosis
CREATE TABLE IF NOT EXISTS dim_diagnosis (
    diagnosis_key SERIAL PRIMARY KEY,
    code_id UUID UNIQUE NOT NULL,
    code VARCHAR(50),
    description TEXT,
    code_system VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_dim_diagnosis_code_id ON dim_diagnosis(code_id);
CREATE INDEX idx_dim_diagnosis_code ON dim_diagnosis(code);

-- Dimension: Time (Date dimension for better date analytics)
CREATE TABLE IF NOT EXISTS dim_time (
    date_key INTEGER PRIMARY KEY,  -- Format: YYYYMMDD
    full_date DATE UNIQUE NOT NULL,
    year INTEGER,
    quarter INTEGER,
    month INTEGER,
    month_name VARCHAR(20),
    week INTEGER,
    day_of_month INTEGER,
    day_of_week INTEGER,
    day_name VARCHAR(20),
    is_weekend BOOLEAN,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_dim_time_date ON dim_time(full_date);
CREATE INDEX idx_dim_time_year_month ON dim_time(year, month);

-- Fact Table: Encounters
CREATE TABLE IF NOT EXISTS fact_encounters (
    encounter_key SERIAL PRIMARY KEY,
    encounter_id UUID UNIQUE NOT NULL,
    patient_key INTEGER REFERENCES dim_patient(patient_key),
    facility_key INTEGER REFERENCES dim_facility(facility_key),
    date_key INTEGER REFERENCES dim_time(date_key),
    encounter_date DATE NOT NULL,
    encounter_type VARCHAR(50),
    num_procedures INTEGER DEFAULT 0,
    num_diagnoses INTEGER DEFAULT 0,
    has_report BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_fact_encounters_patient ON fact_encounters(patient_key);
CREATE INDEX idx_fact_encounters_date ON fact_encounters(date_key);
CREATE INDEX idx_fact_encounters_facility ON fact_encounters(facility_key);

-- Bridge Table: Links encounters to procedures (many-to-many)
CREATE TABLE IF NOT EXISTS bridge_encounter_procedure (
    encounter_key INTEGER REFERENCES fact_encounters(encounter_key),
    procedure_key INTEGER REFERENCES dim_procedure(procedure_key),
    procedure_date TIMESTAMP,
    PRIMARY KEY (encounter_key, procedure_key)
);

-- Bridge Table: Links encounters to diagnoses (many-to-many)
CREATE TABLE IF NOT EXISTS bridge_encounter_diagnosis (
    encounter_key INTEGER REFERENCES fact_encounters(encounter_key),
    diagnosis_key INTEGER REFERENCES dim_diagnosis(diagnosis_key),
    is_primary BOOLEAN DEFAULT FALSE,
    diagnosis_date DATE,
    PRIMARY KEY (encounter_key, diagnosis_key)
);

-- Materialized View: Monthly Encounter Summary
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_monthly_encounters AS
SELECT 
    dt.year,
    dt.month,
    dt.month_name,
    COUNT(DISTINCT fe.encounter_id) as total_encounters,
    COUNT(DISTINCT fe.patient_key) as unique_patients,
    AVG(fe.num_procedures) as avg_procedures_per_encounter,
    SUM(fe.num_procedures) as total_procedures
FROM fact_encounters fe
JOIN dim_time dt ON fe.date_key = dt.date_key
GROUP BY dt.year, dt.month, dt.month_name
ORDER BY dt.year, dt.month;

CREATE INDEX idx_mv_monthly_year_month ON mv_monthly_encounters(year, month);

-- Materialized View: Diagnosis Summary by Age Group
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_diagnosis_by_age_group AS
SELECT 
    dp.age_group,
    dd.code,
    dd.description,
    COUNT(*) as diagnosis_count,
    COUNT(DISTINCT fe.patient_key) as unique_patients
FROM bridge_encounter_diagnosis bed
JOIN fact_encounters fe ON bed.encounter_key = fe.encounter_key
JOIN dim_patient dp ON fe.patient_key = dp.patient_key
JOIN dim_diagnosis dd ON bed.diagnosis_key = dd.diagnosis_key
GROUP BY dp.age_group, dd.code, dd.description
ORDER BY dp.age_group, diagnosis_count DESC;

CREATE INDEX idx_mv_diagnosis_age_group ON mv_diagnosis_by_age_group(age_group);

-- Materialized View: Procedure Volume by Modality
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_procedure_volume AS
SELECT 
    dpr.modality,
    COUNT(*) as procedure_count,
    COUNT(DISTINCT fe.patient_key) as unique_patients,
    COUNT(DISTINCT fe.facility_key) as facilities_performed
FROM bridge_encounter_procedure bep
JOIN fact_encounters fe ON bep.encounter_key = fe.encounter_key
JOIN dim_procedure dpr ON bep.procedure_key = dpr.procedure_key
GROUP BY dpr.modality
ORDER BY procedure_count DESC;

-- Function to refresh all materialized views
CREATE OR REPLACE FUNCTION refresh_all_warehouse_views()
RETURNS void AS $$
BEGIN
    REFRESH MATERIALIZED VIEW mv_monthly_encounters;
    REFRESH MATERIALIZED VIEW mv_diagnosis_by_age_group;
    REFRESH MATERIALIZED VIEW mv_procedure_volume;
END;
$$ LANGUAGE plpgsql;

-- Comments for documentation
COMMENT ON TABLE dim_patient IS 'Patient dimension containing demographic information';
COMMENT ON TABLE dim_facility IS 'Healthcare facility dimension';
COMMENT ON TABLE dim_procedure IS 'Medical procedure dimension';
COMMENT ON TABLE dim_diagnosis IS 'Diagnosis code dimension';
COMMENT ON TABLE dim_time IS 'Date dimension for time-based analytics';
COMMENT ON TABLE fact_encounters IS 'Fact table containing encounter metrics';
COMMENT ON MATERIALIZED VIEW mv_monthly_encounters IS 'Monthly aggregated encounter statistics';
COMMENT ON MATERIALIZED VIEW mv_diagnosis_by_age_group IS 'Diagnosis frequency by patient age groups';
COMMENT ON MATERIALIZED VIEW mv_procedure_volume IS 'Procedure volume statistics by modality';
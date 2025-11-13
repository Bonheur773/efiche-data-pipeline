-- Analytics Queries for eFiche Data Warehouse
-- These queries demonstrate the value of the star schema for clinical decision support

-- ========================================
-- Query 1: Monthly Encounter Trends
-- ========================================
-- Shows encounter volume trends over time
SELECT 
    year,
    month_name,
    total_encounters,
    unique_patients,
    ROUND(avg_procedures_per_encounter, 2) as avg_procedures,
    total_procedures
FROM mv_monthly_encounters
ORDER BY year DESC, month DESC
LIMIT 12;

-- ========================================
-- Query 2: Top Diagnoses by Age Group
-- ========================================
-- Identifies most common diagnoses for each age group
WITH ranked_diagnoses AS (
    SELECT 
        age_group,
        code,
        description,
        diagnosis_count,
        unique_patients,
        ROW_NUMBER() OVER (PARTITION BY age_group ORDER BY diagnosis_count DESC) as rank
    FROM mv_diagnosis_by_age_group
)
SELECT 
    age_group,
    code,
    description,
    diagnosis_count,
    unique_patients
FROM ranked_diagnoses
WHERE rank <= 5
ORDER BY age_group, rank;

-- ========================================
-- Query 3: Procedure Volume by Modality
-- ========================================
-- Shows imaging procedure distribution
SELECT 
    modality,
    procedure_count,
    unique_patients,
    facilities_performed,
    ROUND(procedure_count::NUMERIC / unique_patients, 2) as procedures_per_patient
FROM mv_procedure_volume
ORDER BY procedure_count DESC;

-- ========================================
-- Query 4: Average Procedures per Patient
-- ========================================
-- Overall patient utilization metrics
SELECT 
    COUNT(DISTINCT patient_key) as total_patients,
    COUNT(DISTINCT encounter_key) as total_encounters,
    ROUND(AVG(num_procedures), 2) as avg_procedures_per_encounter,
    ROUND(AVG(num_diagnoses), 2) as avg_diagnoses_per_encounter,
    ROUND(COUNT(DISTINCT encounter_key)::NUMERIC / COUNT(DISTINCT patient_key), 2) as avg_encounters_per_patient
FROM fact_encounters;

-- ========================================
-- Query 5: Weekend vs Weekday Activity
-- ========================================
-- Compares healthcare activity patterns
SELECT 
    dt.is_weekend,
    CASE WHEN dt.is_weekend THEN 'Weekend' ELSE 'Weekday' END as day_type,
    COUNT(DISTINCT fe.encounter_key) as total_encounters,
    AVG(fe.num_procedures) as avg_procedures,
    COUNT(DISTINCT fe.patient_key) as unique_patients
FROM fact_encounters fe
JOIN dim_time dt ON fe.date_key = dt.date_key
GROUP BY dt.is_weekend
ORDER BY dt.is_weekend;

-- ========================================
-- Query 6: Patient Demographics Summary
-- ========================================
-- Breakdown of patient population
SELECT 
    age_group,
    sex,
    COUNT(*) as patient_count,
    ROUND(COUNT(*)::NUMERIC * 100.0 / SUM(COUNT(*)) OVER (), 2) as percentage
FROM dim_patient
GROUP BY age_group, sex
ORDER BY age_group, sex;

-- ========================================
-- Query 7: Facility Performance Comparison
-- ========================================
-- Compares volumes across facilities
SELECT 
    df.facility_name,
    df.facility_type,
    COUNT(DISTINCT fe.encounter_key) as total_encounters,
    COUNT(DISTINCT fe.patient_key) as unique_patients,
    SUM(fe.num_procedures) as total_procedures,
    ROUND(AVG(fe.num_procedures), 2) as avg_procedures_per_encounter
FROM fact_encounters fe
JOIN dim_facility df ON fe.facility_key = df.facility_key
GROUP BY df.facility_name, df.facility_type
ORDER BY total_encounters DESC;

-- ========================================
-- Query 8: High Utilization Patients
-- ========================================
-- Identifies patients with multiple encounters (potential case management candidates)
SELECT 
    dp.patient_key,
    dp.age_group,
    dp.sex,
    COUNT(DISTINCT fe.encounter_key) as encounter_count,
    SUM(fe.num_procedures) as total_procedures,
    COUNT(DISTINCT CASE WHEN fe.has_report THEN fe.encounter_key END) as encounters_with_reports
FROM fact_encounters fe
JOIN dim_patient dp ON fe.patient_key = dp.patient_key
GROUP BY dp.patient_key, dp.age_group, dp.sex
HAVING COUNT(DISTINCT fe.encounter_key) >= 5
ORDER BY encounter_count DESC
LIMIT 20;

-- ========================================
-- Query 9: Diagnosis Co-occurrence Analysis
-- ========================================
-- Finds diagnoses that frequently occur together
SELECT 
    d1.code as diagnosis_1,
    d1.description as description_1,
    d2.code as diagnosis_2,
    d2.description as description_2,
    COUNT(*) as co_occurrence_count
FROM bridge_encounter_diagnosis bed1
JOIN bridge_encounter_diagnosis bed2 
    ON bed1.encounter_key = bed2.encounter_key 
    AND bed1.diagnosis_key < bed2.diagnosis_key
JOIN dim_diagnosis d1 ON bed1.diagnosis_key = d1.diagnosis_key
JOIN dim_diagnosis d2 ON bed2.diagnosis_key = d2.diagnosis_key
GROUP BY d1.code, d1.description, d2.code, d2.description
HAVING COUNT(*) >= 10
ORDER BY co_occurrence_count DESC
LIMIT 10;

-- ========================================
-- Query 10: Quarterly Growth Metrics
-- ========================================
-- Shows quarter-over-quarter growth
SELECT 
    dt.year,
    dt.quarter,
    COUNT(DISTINCT fe.encounter_key) as encounters,
    COUNT(DISTINCT fe.patient_key) as unique_patients,
    SUM(fe.num_procedures) as total_procedures,
    LAG(COUNT(DISTINCT fe.encounter_key)) OVER (ORDER BY dt.year, dt.quarter) as previous_quarter_encounters,
    ROUND(
        (COUNT(DISTINCT fe.encounter_key)::NUMERIC - 
         LAG(COUNT(DISTINCT fe.encounter_key)) OVER (ORDER BY dt.year, dt.quarter)) * 100.0 /
        NULLIF(LAG(COUNT(DISTINCT fe.encounter_key)) OVER (ORDER BY dt.year, dt.quarter), 0),
        2
    ) as growth_percentage
FROM fact_encounters fe
JOIN dim_time dt ON fe.date_key = dt.date_key
GROUP BY dt.year, dt.quarter
ORDER BY dt.year DESC, dt.quarter DESC;

-- ========================================
-- Query 11: Reports Coverage Analysis
-- ========================================
-- Evaluates radiology report completion rates
SELECT 
    dt.year,
    dt.month_name,
    COUNT(*) as total_encounters,
    SUM(CASE WHEN fe.has_report THEN 1 ELSE 0 END) as encounters_with_reports,
    ROUND(
        SUM(CASE WHEN fe.has_report THEN 1 ELSE 0 END)::NUMERIC * 100.0 / COUNT(*),
        2
    ) as report_completion_rate
FROM fact_encounters fe
JOIN dim_time dt ON fe.date_key = dt.date_key
GROUP BY dt.year, dt.month_name, dt.month
ORDER BY dt.year DESC, dt.month DESC
LIMIT 12;

-- ========================================
-- Query 12: Emergency vs Scheduled Encounters
-- ========================================
-- Compares different encounter types
SELECT 
    fe.encounter_type,
    COUNT(*) as encounter_count,
    COUNT(DISTINCT fe.patient_key) as unique_patients,
    ROUND(AVG(fe.num_procedures), 2) as avg_procedures,
    ROUND(AVG(fe.num_diagnoses), 2) as avg_diagnoses,
    SUM(CASE WHEN fe.has_report THEN 1 ELSE 0 END) as encounters_with_reports
FROM fact_encounters fe
GROUP BY fe.encounter_type
ORDER BY encounter_count DESC;
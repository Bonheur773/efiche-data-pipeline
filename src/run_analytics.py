import psycopg2
import pandas as pd
from config import DB_CONFIG
import os

def get_db_connection():
    """Create database connection"""
    return psycopg2.connect(
        host=DB_CONFIG['host'],
        port=DB_CONFIG['port'],
        database=DB_CONFIG['database'],
        user=DB_CONFIG['user'],
        password=DB_CONFIG['password']
    )

def run_query(conn, query_name, query):
    """Execute a query and return results as DataFrame"""
    try:
        df = pd.read_sql_query(query, conn)
        return df
    except Exception as e:
        print(f"Error running {query_name}: {e}")
        return None

def main():
    """Run all analytics queries and display results"""
    print("=" * 70)
    print("eFiche Data Warehouse - Analytics Report")
    print("=" * 70)
    print()
    
    conn = get_db_connection()
    
    # Query 1: Monthly Encounter Trends
    print("📊 QUERY 1: Monthly Encounter Trends")
    print("-" * 70)
    query1 = """
        SELECT year, month_name, total_encounters, unique_patients,
               ROUND(avg_procedures_per_encounter, 2) as avg_procedures
        FROM mv_monthly_encounters
        ORDER BY year DESC, month DESC
        LIMIT 12
    """
    df1 = run_query(conn, "Monthly Trends", query1)
    if df1 is not None:
        print(df1.to_string(index=False))
    print("\n")
    
    # Query 2: Top Diagnoses by Age Group
    print("📊 QUERY 2: Top 3 Diagnoses by Age Group")
    print("-" * 70)
    query2 = """
        WITH ranked_diagnoses AS (
            SELECT age_group, code, description, diagnosis_count,
                   ROW_NUMBER() OVER (PARTITION BY age_group ORDER BY diagnosis_count DESC) as rank
            FROM mv_diagnosis_by_age_group
        )
        SELECT age_group, code, description, diagnosis_count
        FROM ranked_diagnoses
        WHERE rank <= 3
        ORDER BY age_group, rank
    """
    df2 = run_query(conn, "Top Diagnoses", query2)
    if df2 is not None:
        print(df2.to_string(index=False))
    print("\n")
    
    # Query 3: Procedure Volume by Modality
    print("📊 QUERY 3: Procedure Volume by Modality")
    print("-" * 70)
    query3 = """
        SELECT modality, procedure_count, unique_patients,
               ROUND(procedure_count::NUMERIC / unique_patients, 2) as procedures_per_patient
        FROM mv_procedure_volume
        ORDER BY procedure_count DESC
    """
    df3 = run_query(conn, "Procedure Volume", query3)
    if df3 is not None:
        print(df3.to_string(index=False))
    print("\n")
    
    # Query 4: Overall Metrics
    print("📊 QUERY 4: Overall Patient Metrics")
    print("-" * 70)
    query4 = """
        SELECT 
            COUNT(DISTINCT patient_key) as total_patients,
            COUNT(DISTINCT encounter_key) as total_encounters,
            ROUND(AVG(num_procedures), 2) as avg_procedures_per_encounter,
            ROUND(COUNT(DISTINCT encounter_key)::NUMERIC / COUNT(DISTINCT patient_key), 2) as avg_encounters_per_patient
        FROM fact_encounters
    """
    df4 = run_query(conn, "Overall Metrics", query4)
    if df4 is not None:
        print(df4.to_string(index=False))
    print("\n")
    
    # Query 5: Weekend vs Weekday
    print("📊 QUERY 5: Weekend vs Weekday Activity")
    print("-" * 70)
    query5 = """
        SELECT 
            CASE WHEN dt.is_weekend THEN 'Weekend' ELSE 'Weekday' END as day_type,
            COUNT(DISTINCT fe.encounter_key) as total_encounters,
            ROUND(AVG(fe.num_procedures), 2) as avg_procedures,
            COUNT(DISTINCT fe.patient_key) as unique_patients
        FROM fact_encounters fe
        JOIN dim_time dt ON fe.date_key = dt.date_key
        GROUP BY dt.is_weekend
        ORDER BY dt.is_weekend
    """
    df5 = run_query(conn, "Weekend vs Weekday", query5)
    if df5 is not None:
        print(df5.to_string(index=False))
    print("\n")
    
    # Query 6: Patient Demographics
    print("📊 QUERY 6: Patient Demographics")
    print("-" * 70)
    query6 = """
        SELECT age_group, sex, COUNT(*) as patient_count,
               ROUND(COUNT(*)::NUMERIC * 100.0 / SUM(COUNT(*)) OVER (), 2) as percentage
        FROM dim_patient
        GROUP BY age_group, sex
        ORDER BY age_group, sex
    """
    df6 = run_query(conn, "Demographics", query6)
    if df6 is not None:
        print(df6.to_string(index=False))
    print("\n")
    
    # Query 7: Facility Performance
    print("📊 QUERY 7: Top 5 Facilities by Volume")
    print("-" * 70)
    query7 = """
        SELECT df.facility_name, df.facility_type,
               COUNT(DISTINCT fe.encounter_key) as total_encounters,
               SUM(fe.num_procedures) as total_procedures
        FROM fact_encounters fe
        JOIN dim_facility df ON fe.facility_key = df.facility_key
        GROUP BY df.facility_name, df.facility_type
        ORDER BY total_encounters DESC
        LIMIT 5
    """
    df7 = run_query(conn, "Facility Performance", query7)
    if df7 is not None:
        print(df7.to_string(index=False))
    print("\n")
    
    # Query 8: High Utilization Patients
    print("📊 QUERY 8: Top 10 High Utilization Patients")
    print("-" * 70)
    query8 = """
        SELECT dp.age_group, dp.sex,
               COUNT(DISTINCT fe.encounter_key) as encounter_count,
               SUM(fe.num_procedures) as total_procedures
        FROM fact_encounters fe
        JOIN dim_patient dp ON fe.patient_key = dp.patient_key
        GROUP BY dp.patient_key, dp.age_group, dp.sex
        HAVING COUNT(DISTINCT fe.encounter_key) >= 5
        ORDER BY encounter_count DESC
        LIMIT 10
    """
    df8 = run_query(conn, "High Utilization", query8)
    if df8 is not None:
        print(df8.to_string(index=False))
    print("\n")
    
    print("=" * 70)
    print("Analytics Report Complete!")
    print("=" * 70)
    
    conn.close()

if __name__ == "__main__":
    main()
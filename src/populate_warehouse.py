import psycopg2
from datetime import datetime, timedelta
from config import DB_CONFIG

def get_db_connection():
    """Create database connection"""
    return psycopg2.connect(
        host=DB_CONFIG['host'],
        port=DB_CONFIG['port'],
        database=DB_CONFIG['database'],
        user=DB_CONFIG['user'],
        password=DB_CONFIG['password']
    )

def populate_dim_time(conn):
    """Populate time dimension with dates"""
    print("Populating time dimension...")
    cursor = conn.cursor()
    
    # Generate dates from 2 years ago to 1 year in future
    start_date = datetime.now().date() - timedelta(days=730)
    end_date = datetime.now().date() + timedelta(days=365)
    
    current_date = start_date
    inserted_count = 0
    
    while current_date <= end_date:
        date_key = int(current_date.strftime('%Y%m%d'))
        year = current_date.year
        quarter = (current_date.month - 1) // 3 + 1
        month = current_date.month
        month_name = current_date.strftime('%B')
        week = current_date.isocalendar()[1]
        day_of_month = current_date.day
        day_of_week = current_date.weekday() + 1  # 1=Monday, 7=Sunday
        day_name = current_date.strftime('%A')
        is_weekend = day_of_week in [6, 7]
        
        cursor.execute("""
            INSERT INTO dim_time 
            (date_key, full_date, year, quarter, month, month_name, week, 
             day_of_month, day_of_week, day_name, is_weekend)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (full_date) DO NOTHING
        """, (date_key, current_date, year, quarter, month, month_name, week,
              day_of_month, day_of_week, day_name, is_weekend))
        
        if cursor.rowcount > 0:
            inserted_count += 1
        
        current_date += timedelta(days=1)
    
    conn.commit()
    print(f"Inserted {inserted_count} dates into time dimension")

def populate_dim_patient(conn):
    """Populate patient dimension"""
    print("Populating patient dimension...")
    cursor = conn.cursor()
    
    cursor.execute("""
        INSERT INTO dim_patient (patient_id, age, sex, location, age_group)
        SELECT 
            patient_id,
            age,
            sex,
            location,
            CASE 
                WHEN age BETWEEN 18 AND 30 THEN '18-30'
                WHEN age BETWEEN 31 AND 50 THEN '31-50'
                WHEN age BETWEEN 51 AND 70 THEN '51-70'
                WHEN age > 70 THEN '71+'
                ELSE 'Unknown'
            END as age_group
        FROM patients
        ON CONFLICT (patient_id) DO NOTHING
    """)
    
    count = cursor.rowcount
    conn.commit()
    print(f"Inserted {count} patients into dimension")

def populate_dim_facility(conn):
    """Populate facility dimension"""
    print("Populating facility dimension...")
    cursor = conn.cursor()
    
    cursor.execute("""
        INSERT INTO dim_facility (facility_id, facility_name, facility_type, location)
        SELECT facility_id, facility_name, facility_type, location
        FROM facilities
        ON CONFLICT (facility_id) DO NOTHING
    """)
    
    count = cursor.rowcount
    conn.commit()
    print(f"Inserted {count} facilities into dimension")

def populate_dim_procedure(conn):
    """Populate procedure dimension"""
    print("Populating procedure dimension...")
    cursor = conn.cursor()
    
    cursor.execute("""
        INSERT INTO dim_procedure (procedure_id, procedure_name, modality, projection)
        SELECT procedure_id, procedure_name, modality, projection
        FROM procedures
        ON CONFLICT (procedure_id) DO NOTHING
    """)
    
    count = cursor.rowcount
    conn.commit()
    print(f"Inserted {count} procedures into dimension")

def populate_dim_diagnosis(conn):
    """Populate diagnosis dimension"""
    print("Populating diagnosis dimension...")
    cursor = conn.cursor()
    
    cursor.execute("""
        INSERT INTO dim_diagnosis (code_id, code, description, code_system)
        SELECT code_id, code, description, code_system
        FROM diagnosis_codes
        ON CONFLICT (code_id) DO NOTHING
    """)
    
    count = cursor.rowcount
    conn.commit()
    print(f"Inserted {count} diagnosis codes into dimension")

def populate_fact_encounters(conn):
    """Populate fact encounters table"""
    print("Populating fact encounters...")
    cursor = conn.cursor()
    
    # First, insert encounters with their basic info
    cursor.execute("""
        INSERT INTO fact_encounters 
        (encounter_id, patient_key, facility_key, date_key, encounter_date, encounter_type)
        SELECT 
            e.encounter_id,
            dp.patient_key,
            df.facility_key,
            dt.date_key,
            e.encounter_date,
            e.encounter_type
        FROM encounters e
        JOIN dim_patient dp ON e.patient_id = dp.patient_id
        LEFT JOIN dim_facility df ON e.facility_id = df.facility_id
        JOIN dim_time dt ON e.encounter_date = dt.full_date
        ON CONFLICT (encounter_id) DO NOTHING
    """)
    
    count = cursor.rowcount
    print(f"Inserted {count} encounters into fact table")
    
    # Update procedure counts
    cursor.execute("""
        UPDATE fact_encounters fe
        SET num_procedures = (
            SELECT COUNT(*)
            FROM procedures p
            WHERE p.encounter_id = fe.encounter_id
        )
    """)
    
    # Update diagnosis counts
    cursor.execute("""
        UPDATE fact_encounters fe
        SET num_diagnoses = (
            SELECT COUNT(*)
            FROM diagnoses d
            WHERE d.encounter_id = fe.encounter_id
        )
    """)
    
    # Update report flag
    cursor.execute("""
        UPDATE fact_encounters fe
        SET has_report = EXISTS (
            SELECT 1
            FROM reports r
            WHERE r.encounter_id = fe.encounter_id
        )
    """)
    
    conn.commit()
    print("Updated encounter metrics")

def populate_bridge_tables(conn):
    """Populate bridge tables for many-to-many relationships"""
    print("Populating bridge tables...")
    cursor = conn.cursor()
    
    # Bridge: Encounter-Procedure
    cursor.execute("""
        INSERT INTO bridge_encounter_procedure (encounter_key, procedure_key, procedure_date)
        SELECT 
            fe.encounter_key,
            dp.procedure_key,
            p.procedure_date
        FROM procedures p
        JOIN fact_encounters fe ON p.encounter_id = fe.encounter_id
        JOIN dim_procedure dp ON p.procedure_id = dp.procedure_id
        ON CONFLICT DO NOTHING
    """)
    
    proc_count = cursor.rowcount
    print(f"Inserted {proc_count} procedure links")
    
    # Bridge: Encounter-Diagnosis
    cursor.execute("""
        INSERT INTO bridge_encounter_diagnosis (encounter_key, diagnosis_key, is_primary, diagnosis_date)
        SELECT 
            fe.encounter_key,
            dd.diagnosis_key,
            d.is_primary,
            d.diagnosis_date
        FROM diagnoses d
        JOIN fact_encounters fe ON d.encounter_id = fe.encounter_id
        JOIN dim_diagnosis dd ON d.code_id = dd.code_id
        ON CONFLICT DO NOTHING
    """)
    
    diag_count = cursor.rowcount
    conn.commit()
    print(f"Inserted {diag_count} diagnosis links")

def refresh_materialized_views(conn):
    """Refresh all materialized views"""
    print("Refreshing materialized views...")
    cursor = conn.cursor()
    
    cursor.execute("SELECT refresh_all_warehouse_views()")
    conn.commit()
    print("Materialized views refreshed")

def get_warehouse_stats(conn):
    """Get statistics about the warehouse"""
    cursor = conn.cursor()
    
    stats = {}
    
    tables = [
        'dim_patient', 'dim_facility', 'dim_procedure', 'dim_diagnosis', 'dim_time',
        'fact_encounters', 'bridge_encounter_procedure', 'bridge_encounter_diagnosis'
    ]
    
    for table in tables:
        cursor.execute(f"SELECT COUNT(*) FROM {table}")
        stats[table] = cursor.fetchone()[0]
    
    return stats

def main():
    """Main function to populate warehouse"""
    print("=" * 60)
    print("Starting Data Warehouse Population")
    print("=" * 60)
    
    try:
        conn = get_db_connection()
        print("Database connection established")
        
        # Populate dimensions
        populate_dim_time(conn)
        populate_dim_patient(conn)
        populate_dim_facility(conn)
        populate_dim_procedure(conn)
        populate_dim_diagnosis(conn)
        
        # Populate fact table
        populate_fact_encounters(conn)
        
        # Populate bridge tables
        populate_bridge_tables(conn)
        
        # Refresh views
        refresh_materialized_views(conn)
        
        # Show statistics
        print("\n" + "=" * 60)
        print("Warehouse Statistics")
        print("=" * 60)
        stats = get_warehouse_stats(conn)
        for table, count in stats.items():
            print(f"{table}: {count:,}")
        
        print("=" * 60)
        print("Data Warehouse Population Complete!")
        print("=" * 60)
        
        conn.close()
        
    except Exception as e:
        print(f"Error: {e}")
        raise

if __name__ == "__main__":
    main()
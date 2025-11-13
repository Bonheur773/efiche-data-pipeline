import psycopg2
from faker import Faker
import random
from datetime import datetime, timedelta
from config import DB_CONFIG, SYNTHETIC_DATA_CONFIG, COMMON_DIAGNOSES

fake = Faker()

def get_db_connection():
    """Create a connection to the database"""
    return psycopg2.connect(
        host=DB_CONFIG['host'],
        port=DB_CONFIG['port'],
        database=DB_CONFIG['database'],
        user=DB_CONFIG['user'],
        password=DB_CONFIG['password']
    )

def generate_facilities(conn, num_facilities=10):
    """Generate fake healthcare facilities"""
    print(f"Generating {num_facilities} facilities...")
    cursor = conn.cursor()
    
    facility_types = ['Hospital', 'Clinic', 'Medical Center', 'Urgent Care']
    facility_ids = []
    
    for _ in range(num_facilities):
        facility_name = fake.company() + ' Medical Center'
        facility_type = random.choice(facility_types)
        location = fake.city() + ', ' + fake.state_abbr()
        
        cursor.execute("""
            INSERT INTO facilities (facility_name, facility_type, location)
            VALUES (%s, %s, %s)
            RETURNING facility_id
        """, (facility_name, facility_type, location))
        
        facility_id = cursor.fetchone()[0]
        facility_ids.append(facility_id)
    
    conn.commit()
    print(f"Created {len(facility_ids)} facilities")
    return facility_ids

def generate_diagnosis_codes(conn):
    """Insert standard diagnosis codes"""
    print("Adding diagnosis codes...")
    cursor = conn.cursor()
    
    for code, description in COMMON_DIAGNOSES:
        cursor.execute("""
            INSERT INTO diagnosis_codes (code, description, code_system)
            VALUES (%s, %s, %s)
            ON CONFLICT (code) DO NOTHING
            RETURNING code_id
        """, (code, description, 'ICD-10'))
    
    # Get all code_ids
    cursor.execute("SELECT code_id FROM diagnosis_codes")
    code_ids = [row[0] for row in cursor.fetchall()]
    
    conn.commit()
    print(f"Added {len(code_ids)} diagnosis codes")
    return code_ids

def generate_patients(conn, num_patients=5000):
    """Generate fake patient records"""
    print(f"Generating {num_patients} patients...")
    cursor = conn.cursor()
    patient_ids = []
    
    for i in range(num_patients):
        age = random.randint(18, 90)
        sex = random.choice(['Male', 'Female', 'Other'])
        location = fake.city() + ', ' + fake.state_abbr()
        
        cursor.execute("""
            INSERT INTO patients (age, sex, location)
            VALUES (%s, %s, %s)
            RETURNING patient_id
        """, (age, sex, location))
        
        patient_id = cursor.fetchone()[0]
        patient_ids.append(patient_id)
        
        if (i + 1) % 1000 == 0:
            print(f"  Created {i + 1} patients...")
            conn.commit()
    
    conn.commit()
    print(f"Created {len(patient_ids)} patients")
    return patient_ids

def generate_encounters_and_procedures(conn, patient_ids, facility_ids, code_ids):
    """Generate encounters with associated procedures and diagnoses"""
    print("Generating encounters, procedures, and diagnoses...")
    cursor = conn.cursor()
    
    modalities = ['X-Ray', 'CT', 'MRI', 'Ultrasound']
    projections = ['PA', 'AP', 'Lateral', 'Oblique']
    encounter_types = ['Inpatient', 'Outpatient', 'Emergency']
    
    total_encounters = 0
    total_procedures = 0
    total_diagnoses = 0
    
    for patient_id in patient_ids:
        # Each patient gets 1-8 encounters
        num_encounters = random.randint(*SYNTHETIC_DATA_CONFIG['encounters_per_patient_range'])
        
        for _ in range(num_encounters):
            # Create encounter
            encounter_date = fake.date_between(start_date='-2y', end_date='today')
            facility_id = random.choice(facility_ids)
            encounter_type = random.choice(encounter_types)
            
            cursor.execute("""
                INSERT INTO encounters (patient_id, facility_id, encounter_date, encounter_type)
                VALUES (%s, %s, %s, %s)
                RETURNING encounter_id
            """, (patient_id, facility_id, encounter_date, encounter_type))
            
            encounter_id = cursor.fetchone()[0]
            total_encounters += 1
            
            # Create 1-3 procedures per encounter
            num_procedures = random.randint(*SYNTHETIC_DATA_CONFIG['procedures_per_encounter_range'])
            
            for _ in range(num_procedures):
                modality = random.choice(modalities)
                projection = random.choice(projections)
                procedure_name = f'{modality} {projection} Chest'
                
                cursor.execute("""
                    INSERT INTO procedures (encounter_id, procedure_name, modality, projection)
                    VALUES (%s, %s, %s, %s)
                """, (encounter_id, procedure_name, modality, projection))
                
                total_procedures += 1
            
            # Add 1-2 diagnoses per encounter
            num_diagnoses = random.randint(1, 2)
            selected_codes = random.sample(code_ids, min(num_diagnoses, len(code_ids)))
            
            for idx, code_id in enumerate(selected_codes):
                cursor.execute("""
                    INSERT INTO diagnoses (encounter_id, code_id, diagnosis_date, is_primary)
                    VALUES (%s, %s, %s, %s)
                """, (encounter_id, code_id, encounter_date, idx == 0))
                
                total_diagnoses += 1
        
        # Commit every 100 patients to avoid memory issues
        if total_encounters % 500 == 0:
            conn.commit()
            print(f"  Progress: {total_encounters} encounters, {total_procedures} procedures, {total_diagnoses} diagnoses")
    
    conn.commit()
    print(f"Created {total_encounters} encounters")
    print(f"Created {total_procedures} procedures")
    print(f"Created {total_diagnoses} diagnoses")

def main():
    """Main function to generate all synthetic data"""
    print("=" * 50)
    print("Starting Synthetic Data Generation")
    print("=" * 50)
    
    try:
        conn = get_db_connection()
        print("Database connection established")
        
        # Step 1: Generate facilities
        facility_ids = generate_facilities(conn, SYNTHETIC_DATA_CONFIG['num_patients'] // 500)
        
        # Step 2: Add diagnosis codes
        code_ids = generate_diagnosis_codes(conn)
        
        # Step 3: Generate patients
        patient_ids = generate_patients(conn, SYNTHETIC_DATA_CONFIG['num_patients'])
        
        # Step 4: Generate encounters, procedures, and diagnoses
        generate_encounters_and_procedures(conn, patient_ids, facility_ids, code_ids)
        
        print("=" * 50)
        print("Synthetic Data Generation Complete!")
        print("=" * 50)
        
        conn.close()
        
    except Exception as e:
        print(f"Error: {e}")
        raise

if __name__ == "__main__":
    main()
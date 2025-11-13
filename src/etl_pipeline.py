import psycopg2
import pandas as pd
from datasets import load_dataset
import random
from datetime import datetime, timedelta
from config import DB_CONFIG, PADCHEST_CONFIG
import os
import json

def get_db_connection():
    """Create database connection"""
    return psycopg2.connect(
        host=DB_CONFIG['host'],
        port=DB_CONFIG['port'],
        database=DB_CONFIG['database'],
        user=DB_CONFIG['user'],
        password=DB_CONFIG['password']
    )

def download_padchest_data(sample_size=10000):
    """Download PadChest dataset from HuggingFace"""
    print(f"Downloading PadChest dataset (sample of {sample_size} records)...")
    print("This might take a few minutes on first download...")
    
    try:
        # Load dataset from HuggingFace
        dataset = load_dataset(PADCHEST_CONFIG['dataset_name'], split='train')
        
        # Take a sample
        if len(dataset) > sample_size:
            dataset = dataset.shuffle(seed=42).select(range(sample_size))
        
        # Convert to pandas for easier processing
        df = pd.DataFrame(dataset)
        
        print(f"Downloaded {len(df)} records")
        return df
    
    except Exception as e:
        print(f"Error downloading dataset: {e}")
        print("Creating sample data instead...")
        return create_sample_padchest_data(sample_size)

def create_sample_padchest_data(sample_size):
    """Create sample data if download fails"""
    print(f"Creating {sample_size} sample records...")
    
    data = []
    labels_list = ['pneumonia', 'edema', 'atelectasis', 'normal', 'pleural effusion']
    
    for i in range(sample_size):
        data.append({
            'ImageID': f'IMG_{i:06d}',
            'PatientAge': random.randint(20, 85),
            'PatientSex': random.choice(['M', 'F']),
            'StudyDate': (datetime.now() - timedelta(days=random.randint(0, 730))).strftime('%Y%m%d'),
            'Projection': random.choice(['PA', 'AP', 'L']),
            'Modality': 'DX',
            'Labels': random.choice(labels_list),
            'ReportText': f'Chest X-ray shows {random.choice(labels_list)}'
        })
    
    return pd.DataFrame(data)

def load_to_staging(conn, df):
    """Load PadChest data to staging table"""
    print(f"Loading {len(df)} records to staging table...")
    cursor = conn.cursor()
    
    loaded_count = 0
    duplicate_count = 0
    
    for _, row in df.iterrows():
        try:
            # Extract fields with proper defaults
            image_id = str(row.get('ImageID', f'IMG_{loaded_count}'))
            patient_age = int(row.get('PatientAge', 0)) if pd.notna(row.get('PatientAge')) else None
            patient_sex = str(row.get('PatientSex', 'Unknown'))
            
            # Handle study date
            study_date_str = str(row.get('StudyDate', ''))
            try:
                study_date = datetime.strptime(study_date_str, '%Y%m%d').date() if study_date_str else None
            except:
                study_date = None
            
            projection = str(row.get('Projection', 'PA'))
            modality = str(row.get('Modality', 'DX'))
            labels = str(row.get('Labels', ''))
            report_text = str(row.get('ReportText', ''))
            
            # Insert into staging
            cursor.execute("""
                INSERT INTO padchest_staging 
                (image_id, patient_age, patient_sex, study_date, projection, modality, labels, report_text)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (image_id) DO NOTHING
            """, (image_id, patient_age, patient_sex, study_date, projection, modality, labels, report_text))
            
            if cursor.rowcount > 0:
                loaded_count += 1
            else:
                duplicate_count += 1
            
            # Commit in batches
            if loaded_count % 1000 == 0:
                conn.commit()
                print(f"  Loaded {loaded_count} records...")
        
        except Exception as e:
            print(f"Error loading row: {e}")
            continue
    
    conn.commit()
    print(f"Loaded {loaded_count} new records to staging")
    print(f"Skipped {duplicate_count} duplicates")
    return loaded_count

def process_staging_to_production(conn):
    """Process staging data and insert into production tables"""
    print("Processing staging data into production tables...")
    cursor = conn.cursor()
    
    # Get unprocessed records
    cursor.execute("""
        SELECT staging_id, image_id, patient_age, patient_sex, study_date, 
               projection, modality, labels, report_text
        FROM padchest_staging
        WHERE processed = FALSE
        ORDER BY study_date DESC NULLS LAST
        LIMIT 5000
    """)
    
    records = cursor.fetchall()
    print(f"Found {len(records)} unprocessed records")
    
    if len(records) == 0:
        print("No new records to process")
        return
    
    # Get random existing patients to match with
    cursor.execute("SELECT patient_id FROM patients ORDER BY RANDOM() LIMIT 1000")
    patient_pool = [row[0] for row in cursor.fetchall()]
    
    # Get facilities
    cursor.execute("SELECT facility_id FROM facilities ORDER BY RANDOM() LIMIT 5")
    facility_pool = [row[0] for row in cursor.fetchall()]
    
    processed_count = 0
    
    for record in records:
        staging_id, image_id, patient_age, patient_sex, study_date, projection, modality, labels, report_text = record
        
        try:
            # Match to a random existing patient
            patient_id = random.choice(patient_pool)
            facility_id = random.choice(facility_pool)
            
            # Use study_date or generate a random recent date
            encounter_date = study_date if study_date else datetime.now().date()
            
            # Create encounter
            cursor.execute("""
                INSERT INTO encounters (patient_id, facility_id, encounter_date, encounter_type, status)
                VALUES (%s, %s, %s, %s, %s)
                RETURNING encounter_id
            """, (patient_id, facility_id, encounter_date, 'Outpatient', 'completed'))
            
            encounter_id = cursor.fetchone()[0]
            
            # Create procedure
            cursor.execute("""
                INSERT INTO procedures (encounter_id, procedure_name, modality, projection)
                VALUES (%s, %s, %s, %s)
            """, (encounter_id, f'{modality} Chest Imaging', modality, projection))
            
            # Create report
            cursor.execute("""
                INSERT INTO reports (encounter_id, report_type, report_text, language)
                VALUES (%s, %s, %s, %s)
            """, (encounter_id, 'radiology', report_text, 'en'))
            
            # Mark as processed
            cursor.execute("""
                UPDATE padchest_staging
                SET processed = TRUE
                WHERE staging_id = %s
            """, (staging_id,))
            
            processed_count += 1
            
            # Commit in batches
            if processed_count % 500 == 0:
                conn.commit()
                print(f"  Processed {processed_count} records...")
        
        except Exception as e:
            print(f"Error processing record {image_id}: {e}")
            conn.rollback()
            continue
    
    conn.commit()
    print(f"Successfully processed {processed_count} records")

def get_pipeline_stats(conn):
    """Get statistics about the data pipeline"""
    cursor = conn.cursor()
    
    stats = {}
    
    # Staging stats
    cursor.execute("SELECT COUNT(*) FROM padchest_staging")
    stats['total_staging'] = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM padchest_staging WHERE processed = TRUE")
    stats['processed_staging'] = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM padchest_staging WHERE processed = FALSE")
    stats['unprocessed_staging'] = cursor.fetchone()[0]
    
    # Production stats
    cursor.execute("SELECT COUNT(*) FROM patients")
    stats['total_patients'] = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM encounters")
    stats['total_encounters'] = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM procedures")
    stats['total_procedures'] = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM reports")
    stats['total_reports'] = cursor.fetchone()[0]
    
    return stats

def run_etl_pipeline():
    """Main ETL pipeline execution"""
    print("=" * 60)
    print("Starting ETL Pipeline")
    print("=" * 60)
    
    try:
        # Step 1: Download PadChest data
        df = download_padchest_data(PADCHEST_CONFIG['sample_size'])
        
        # Step 2: Connect to database
        conn = get_db_connection()
        print("Database connection established")
        
        # Step 3: Load to staging
        loaded_count = load_to_staging(conn, df)
        
        # Step 4: Process staging to production
        if loaded_count > 0:
            process_staging_to_production(conn)
        
        # Step 5: Show statistics
        print("\n" + "=" * 60)
        print("Pipeline Statistics")
        print("=" * 60)
        stats = get_pipeline_stats(conn)
        for key, value in stats.items():
            print(f"{key}: {value:,}")
        
        print("=" * 60)
        print("ETL Pipeline Complete!")
        print("=" * 60)
        
        conn.close()
        
    except Exception as e:
        print(f"Pipeline error: {e}")
        raise

if __name__ == "__main__":
    run_etl_pipeline()
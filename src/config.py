import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Database connection settings
DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': os.getenv('DB_PORT', '5433'),  # Using 5433 since we changed it
    'database': os.getenv('DB_NAME', 'efiche_db'),
    'user': os.getenv('DB_USER', 'efiche_user'),
    'password': os.getenv('DB_PASSWORD', 'efiche_pass')
}

# Data generation settings
SYNTHETIC_DATA_CONFIG = {
    'num_patients': 5000,
    'num_facilities': 10,
    'encounters_per_patient_range': (1, 8),  # min and max encounters per patient
    'procedures_per_encounter_range': (1, 3)
}

# PadChest dataset settings
PADCHEST_CONFIG = {
    'dataset_name': 'MedHK23/IMT-CXR',
    'sample_size': 10000,
    'batch_size': 100
}

# Common diagnosis codes for radiology
COMMON_DIAGNOSES = [
    ('J18.9', 'Pneumonia, unspecified organism'),
    ('J98.11', 'Atelectasis'),
    ('I50.9', 'Heart failure, unspecified'),
    ('J81.0', 'Acute pulmonary edema'),
    ('J44.1', 'Chronic obstructive pulmonary disease with acute exacerbation'),
    ('R91.8', 'Other nonspecific abnormal finding of lung field'),
    ('J96.00', 'Acute respiratory failure'),
    ('J18.1', 'Lobar pneumonia'),
    ('I27.20', 'Pulmonary hypertension, unspecified'),
    ('J84.10', 'Pulmonary fibrosis, unspecified')
]
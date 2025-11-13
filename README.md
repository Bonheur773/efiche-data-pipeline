# eFiche Data Engineering Assessment

A comprehensive data pipeline and analytics platform for clinical decision support, demonstrating enterprise-grade data engineering practices with PostgreSQL, Python, and Docker.

## 📋 Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Project Structure](#project-structure)
- [Setup Instructions](#setup-instructions)
- [Components](#components)
- [Analytics Queries](#analytics-queries)
- [Design Decisions](#design-decisions)
- [Future Enhancements](#future-enhancements)

## 🎯 Overview

This project implements a scalable healthcare data pipeline that:
- Ingests patient encounter data from external sources (PadChest dataset)
- Stores data in a normalized relational database
- Transforms data into a star schema data warehouse
- Provides analytics for clinical decision support

**Key Metrics:**
- 5,000+ synthetic patients
- 27,000+ clinical encounters
- 50,000+ medical procedures
- 33,000+ diagnoses
- 10 healthcare facilities

## 🏗️ Architecture

### Data Flow
```
External Source (PadChest) 
    ↓
Staging Layer (padchest_staging)
    ↓
Operational Database (normalized tables)
    ↓
Data Warehouse (star schema)
    ↓
Analytics & Reporting
```

### Technology Stack
- **Database**: PostgreSQL 15
- **Language**: Python 3.11+
- **Containerization**: Docker & Docker Compose
- **Libraries**: 
  - `psycopg2` - PostgreSQL adapter
  - `pandas` - Data manipulation
  - `faker` - Synthetic data generation
  - `datasets` - HuggingFace dataset integration
  - `sqlalchemy` - Database toolkit

## 📁 Project Structure

```
efiche-data-pipeline/
├── README.md
├── docker-compose.yml          # Container orchestration
├── requirements.txt            # Python dependencies
├── .env                       # Environment variables
├── .gitignore
│
├── sql/                       # Database schemas
│   ├── 01_schema.sql         # Operational database schema
│   ├── 03_warehouse.sql      # Data warehouse star schema
│   └── analytics_queries.sql # Sample analytical queries
│
├── src/                       # Python source code
│   ├── __init__.py
│   ├── config.py             # Configuration management
│   ├── generate_synthetic_data.py  # Creates fake patient data
│   ├── etl_pipeline.py       # Extracts and loads PadChest data
│   ├── populate_warehouse.py # Populates data warehouse
│   └── analytics_queries.py  # Python query utilities
│
├── data/                      # Data files (gitignored)
│   └── .gitkeep
│
└── logs/                      # Pipeline logs
    └── .gitkeep
```

## 🚀 Setup Instructions

### Prerequisites
- Docker Desktop installed and running
- Python 3.9+ 
- Git

### 1. Clone the Repository
```bash
git clone <repository-url>
cd efiche-data-pipeline
```

### 2. Start Database Containers
```bash
docker-compose up -d
```

This starts:
- PostgreSQL database on `localhost:5433`
- PgAdmin interface on `localhost:5050`

### 3. Set Up Python Environment
```bash
# Create virtual environment
python -m venv venv

# Activate (Windows)
venv\Scripts\activate

# Activate (Mac/Linux)
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

### 4. Create Database Schema
The schema is automatically created when Docker starts. To verify:
```bash
docker exec -it efiche_postgres psql -U efiche_user -d efiche_db -c "\dt"
```

### 5. Generate Synthetic Data
```bash
python src/generate_synthetic_data.py
```
**Output**: 5,000 patients, 22,000+ encounters, 45,000+ procedures

### 6. Run ETL Pipeline
```bash
python src/etl_pipeline.py
```
**Output**: Loads 10,000 PadChest records, processes 5,000 into production

### 7. Populate Data Warehouse
```bash
python src/populate_warehouse.py
```
**Output**: Creates star schema with dimension and fact tables

### 8. Access PgAdmin (Optional)
1. Open browser: `http://localhost:5050`
2. Login: `admin@efiche.com` / `admin`
3. Add server connection:
   - Host: `postgres`
   - Port: `5432`
   - Database: `efiche_db`
   - Username: `efiche_user`
   - Password: `efiche_pass`

## 🔧 Components

### Part 1: Data Modeling

**Operational Schema (Normalized)**

Core tables designed for transactional integrity:

- **patients**: Patient demographics (age, sex, location)
- **facilities**: Healthcare facilities
- **encounters**: Patient visits to facilities
- **procedures**: Medical procedures performed
- **diagnosis_codes**: Standard ICD-10 diagnosis codes
- **diagnoses**: Links encounters to diagnosis codes
- **reports**: Clinical reports (radiology, pathology)
- **padchest_staging**: Staging area for external data
- **audit_log**: Change tracking

**Design Rationale:**
- UUID primary keys for distributed system compatibility
- Foreign keys enforce referential integrity
- Indexes on frequently queried columns
- Prepared for future embeddings (vector column in reports table)
- Staging table isolates external data before validation

### Part 2: ETL Pipeline

**Continuous Data Ingestion**

The pipeline (`src/etl_pipeline.py`) implements:

1. **Extract**: Downloads PadChest dataset from HuggingFace
2. **Load to Staging**: Inserts raw data into `padchest_staging`
3. **Transform**: 
   - Deduplicates using `image_id`
   - Randomly matches to existing patients
   - Creates encounters, procedures, and reports
4. **Incremental Processing**: Tracks processed records with boolean flag

**Key Features:**
- Batch processing (500 records at a time)
- Duplicate handling with `ON CONFLICT` clauses
- Error handling and logging
- Idempotent - can be run multiple times safely

**Running Incrementally:**
```bash
# First run processes 5,000 records
python src/etl_pipeline.py

# Run again to process next batch
python src/etl_pipeline.py
```

### Part 3: Data Warehouse

**Star Schema Design**

Optimized for analytical queries:

**Dimension Tables:**
- `dim_patient` - Patient attributes with derived age_group
- `dim_facility` - Facility details
- `dim_procedure` - Procedure catalog
- `dim_diagnosis` - Diagnosis code reference
- `dim_time` - Date dimension (1,095 days)

**Fact Table:**
- `fact_encounters` - Central fact table with metrics:
  - `num_procedures` - Count of procedures per encounter
  - `num_diagnoses` - Count of diagnoses
  - `has_report` - Report availability flag

**Bridge Tables:**
- `bridge_encounter_procedure` - Many-to-many: encounters ↔ procedures
- `bridge_encounter_diagnosis` - Many-to-many: encounters ↔ diagnoses

**Materialized Views** (Pre-aggregated for performance):
- `mv_monthly_encounters` - Monthly statistics
- `mv_diagnosis_by_age_group` - Diagnosis distribution
- `mv_procedure_volume` - Procedure counts by modality

**Refreshing Views:**
```sql
SELECT refresh_all_warehouse_views();
```

## 📊 Analytics Queries

Sample queries in `sql/analytics_queries.sql`:

### Query 1: Monthly Encounter Trends
```sql
SELECT year, month, month_name, total_encounters, unique_patients
FROM mv_monthly_encounters
ORDER BY year DESC, month DESC;
```

### Query 2: Top Diagnoses by Age Group
```sql
SELECT age_group, code, description, diagnosis_count
FROM mv_diagnosis_by_age_group
WHERE age_group = '51-70'
ORDER BY diagnosis_count DESC
LIMIT 10;
```

### Query 3: Average Procedures per Patient
```sql
SELECT 
    COUNT(DISTINCT patient_key) as total_patients,
    ROUND(AVG(num_procedures), 2) as avg_procedures_per_encounter
FROM fact_encounters;
```

### Query 4: High-Volume Patients (Chronic Care Candidates)
```sql
SELECT p.patient_key, p.age, COUNT(*) as total_visits
FROM fact_encounters fe
JOIN dim_patient p ON fe.patient_key = p.patient_key
GROUP BY p.patient_key, p.age
HAVING COUNT(*) >= 5
ORDER BY total_visits DESC;
```

**Run all queries:**
```bash
# In psql
\i sql/analytics_queries.sql

# Or in PgAdmin
# Open file → Execute
```

## 💡 Design Decisions

### 1. **Docker for Reproducibility**
- Eliminates "works on my machine" issues
- Consistent PostgreSQL version across environments
- Easy teardown and recreation

### 2. **Staging Layer Pattern**
- Isolates raw external data
- Allows validation before production
- Enables reprocessing if needed
- Tracks processing status

### 3. **Star Schema for Analytics**
- Optimized for read-heavy workloads
- Intuitive for business users
- Supports complex aggregations
- Materialized views pre-compute common metrics

### 4. **Synthetic Data Generation**
- Creates realistic test dataset
- Enables end-to-end testing without PHI
- Configurable volume (adjust in `config.py`)

### 5. **Incremental Load Strategy**
- `processed` flag tracks staging records
- Batch processing prevents memory issues
- Can resume after failures
- Production-ready pattern

### 6. **UUID vs Sequential IDs**
- UUIDs enable distributed systems
- No coordination needed for ID generation
- Merging data from multiple sources is easier
- Trade-off: Slightly larger storage (acceptable)

### 7. **Future-Proofing**
- `text_embedding` column for ML models
- Audit log table for compliance
- Flexible schema allows new data types
- Modular codebase for extensions

## 🚀 Future Enhancements

### Immediate Next Steps
1. **Text Embeddings**: Generate embeddings for report text using sentence transformers
2. **Scheduled Pipeline**: Use Apache Airflow for automated daily runs
3. **Data Quality Checks**: Add Great Expectations validation
4. **Dashboard**: Create Grafana/Superset visualizations

### Advanced Features
1. **Real-time Streaming**: Kafka integration for live data
2. **ML Model Training**: Predict diagnoses from report text
3. **API Layer**: FastAPI for data access
4. **Multi-tenancy**: Support multiple hospitals/systems
5. **CDC (Change Data Capture)**: Track all changes in source tables
6. **Data Versioning**: Implement slowly changing dimensions (SCD Type 2)

## 🧪 Testing Your Setup

### Verify Database
```bash
docker exec -it efiche_postgres psql -U efiche_user -d efiche_db
```

```sql
-- Check record counts
SELECT 'patients' as table_name, COUNT(*) FROM patients
UNION ALL
SELECT 'encounters', COUNT(*) FROM encounters
UNION ALL
SELECT 'fact_encounters', COUNT(*) FROM fact_encounters;
```

### Verify Warehouse
```sql
-- Quick analytics test
SELECT 
    age_group,
    COUNT(DISTINCT patient_key) as patient_count
FROM dim_patient
GROUP BY age_group
ORDER BY age_group;
```

## 📝 Environment Variables

Create `.env` file:
```bash
DB_HOST=localhost
DB_PORT=5433
DB_NAME=efiche_db
DB_USER=efiche_user
DB_PASSWORD=efiche_pass
```

## 🐛 Troubleshooting

**Docker won't start:**
```bash
# Check Docker Desktop is running
docker info

# Restart containers
docker-compose down
docker-compose up -d
```

**Port conflict (5432 already in use):**
- We use port 5433 in this project
- Check `docker-compose.yml` ports section

**Python packages not installing:**
```bash
# Ensure venv is activated
python -m pip install --upgrade pip
pip install -r requirements.txt
```

**Database connection refused:**
```bash
# Check container status
docker ps

# Check logs
docker logs efiche_postgres
```

## 📄 License

This project is for assessment purposes only.

## 👤 Author

Developed as part of the eFiche Senior Data Engineer assessment.

---

**Assessment Completed**: ✅ All three parts implemented
- Part 1: Data modeling with normalized schema
- Part 2: ETL pipeline with continuous ingestion
- Part 3: Data warehouse with star schema and analytics
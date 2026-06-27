# Health Data Pipeline - Promptly Health 

A production-grade ELT pipeline that ingests patient data from Google Sheets, transforms it into a standardised **FHIR Patient** format, and loads it into Snowflake inclusive of orchestration.

---

The raw data for this project is available in this [Google Sheets file](https://docs.google.com/spreadsheets/d/1zZA8j-MR3osXR9-jgz9q-m3_sdrFDi72B4d3BBMTfrs/edit?usp=sharing), where the original CSV has been uploaded.

## Architecture

```
Google Sheets: Raw Patient Data CSV 
     │
     ▼
┌─────────────────────────────────────────────────────┐
│  **RAW Schema**                                     │
│  raw_raw_patient (3 columns)                        │
│  loaded_at | source_sheet_id | raw_data (VARIANT)   │
│  Tests: metadata not null, required JSON keys       │                   
└─────────────────────────────────────────────────────┘
     │
     ▼
┌─────────────────────────────────────────────────────┐
│  **UAT Schema**                                     │
│  uat_patient                                        │
│  Extracts from JSON, applies FHIR R4 mapping        │
│  Tests: patient ID unique, gender required binding, │
│         telecom valid, marital status v3 codes      │
└─────────────────────────────────────────────────────┘
     │
     ▼
┌─────────────────────────────────────────────────────┐
│  **CONSUMPTION Schema**                             │
│  consumption_fhir_patient                           │
│  Final FHIR    Patient resource                     │
│  PII masked, telecom as ContactPoint array          │
└─────────────────────────────────────────────────────┘
     │
     ▼
┌─────────────────────────────────────────────────────┐
│  **Prefect Cloud**                                  │
│  Scheduled CRON Job — lint → ingest → run → test    │
└─────────────────────────────────────────────────────┘
```

---

## Tech Stack

| Layer | Tool | Purpose |
|---|---|---|
| Source | Google Sheets + GCP Service Account | Data source |
| Ingestion | Python + gspread + snowflake-connector | Extract and load |
| Storage | Snowflake (dev ) | Cloud data warehouse |
| Transformation | dbt Core + dbt_utils | Layered SQL models |
| Data Quality | dbt Tests (yml + custom macros) | FHIR R4 aligned checks |
| SQL Linting | SQLFluff | SQL code quality |
| PII Protection | MD5 Hashing + Column Masking | HIPAA-aligned protection |
| Orchestration | Prefect Cloud | Scheduling and monitoring |
| Version Control | GitHub | Source code management |

---

## Project Structure

```
health-data-pipeline/
├── ingestion/
│   └── load_data.py                        # Google Sheet → Snowflake raw
├── dbt_project/
│   └── health_pipeline/
│       ├── models/
│       │   ├── raw/
│       │   │   └── schema.yml              # Raw layer tests
│       │   ├── uat/
│       │   │   ├── schema.yml              # UAT FHIR compliance tests
│       │   │   └── uat_patient.sql         # FHIR R4 mapping
│       │   └── consumption/
│       │       └── consumption_fhir_patient.sql
│       ├── macros/
│       │   ├── create_schemas.sql          # Auto-creates all schemas
│       │   ├── generate_schema_name.sql
│       │   ├── test_json_key_not_null.sql  # Custom test macro
│       ├── packages.yml
│       ├── dbt_project.yml
│       └── profiles.yml                   # git ignored
├── orchestration/
│   ├── pipeline_flow.py                   # Prefect flow
├── scripts/
│   ├── init_db.py                         # One-time Snowflake init
│   └── setup.py                           # Full environment setup
├── credentials/                           # git ignored
├── .env                                   # git ignored
├── .gitignore
├── prefect.yaml
├── requirements.txt
└── README.md
```

---

---

## FHIR R4 Data Quality Tests

### RAW Layer — Structural checks (ERROR if failing)

| Test | Description | FHIR Reference |
|---|---|---|
| `loaded_at` not null | Metadata integrity | Pipeline rule |
| `source_sheet_id` not null | Audit trail | Pipeline rule |
| `raw_data` not null | Payload present | Pipeline rule |
| `first_name` key present | Required for Patient.name | Business rule |
| `last_name` key present | Required for Patient.name | Business rule |
| `gender` key present | Required for Patient.gender | Business rule |
| `birth_date` key present | Required for Patient.birthDate | Business rule |
| `phone_number` key present | Required for Patient.telecom | Business rule |
| `email` key present | Required for Patient.telecom | Business rule |
| `marital_status` key present | Required for Patient.maritalStatus | Business rule |

### UAT Layer — FHIR R4 compliance checks

| Test | Severity | FHIR Reference |
|---|---|---|
| `patient_id` unique | ERROR | [Resource.id](https://hl7.org/fhir/R4/resource.html#id) |
| `patient_id` not null | ERROR | [Resource.id](https://hl7.org/fhir/R4/resource.html#id) |
| `gender` not null | ERROR | [AdministrativeGender](https://hl7.org/fhir/R4/valueset-administrative-gender.html) |
| `gender` in male/female/other/unknown | ERROR | Required binding |
| `telecom_phone` not null | ERROR | [Patient.telecom](https://hl7.org/fhir/R4/patient-definitions.html#Patient.telecom) |
| `telecom_email` not null | ERROR | [Patient.telecom](https://hl7.org/fhir/R4/patient-definitions.html#Patient.telecom) |
| `birth_date` not future | Date type constraint | [Patient.birthDate](https://hl7.org/fhir/R4/patient-definitions.html#Patient.birthDate) |
| `phone_hash` not null | WARN | 0..* cardinality |
| `email_hash` not null | WARN | 0..* cardinality |
| `marital_status_code` in M/S/D/W/UNK | WARN | [v3-MaritalStatus](https://hl7.org/fhir/R4/valueset-marital-status.html) Extensible binding |
| `insurance_number_hash` not null | WARN | Pipeline rule |

---

## Data Protection

PII fields are protected at the **UAT layer** before reaching consumption:

| Field | Treatment |
|---|---|
| `first_name`, `last_name` | MD5 hashed |
| `email`, `phone_number` | MD5 hashed |
| `insurance_number` | MD5 hashed |
| `birth_date` | Generalised to year only |
| `address`, `full_name` | MD5 hashed |

---

## Prerequisites

Install the following before starting:

### (Step 1 to 3 -> Optional Installation but highly recommended)

### 1. Git
```bash
git --version
```
Download from [git-scm.com](https://git-scm.com/downloads) if not installed.

### 2. Miniconda
```bash
conda --version
```
Download from [docs.conda.io/en/latest/miniconda.html](https://docs.conda.io/en/latest/miniconda.html) if not installed.

### 3. Visual Studio Code
Download from [code.visualstudio.com](https://code.visualstudio.com).

Recommended extensions: Python, dbt Power User, SQLFluff

### 4. Snowflake Account
Sign up for a free trial at [signup.snowflake.com](https://signup.snowflake.com).
- Select **Standard** edition
- Choose **AWS** as cloud provider
- Note your **account identifier** from the bottom left of the Snowflake UI

### 5. Snowflake CLI
```bash
pip install snowflake-cli-labs
snow --version
```

### 6. Prefect Cloud Account
Sign up for free at [app.prefect.cloud](https://app.prefect.cloud).

---

## Reproducibility steps

Follow these steps exactly in order.

### Step 1 — Clone the repository

```bash
git clone https://github.com/DarvinSures/health-data-pipeline.git
cd health-data-pipeline
```

### Step 2 — Create and activate conda environment

```bash
conda create -n health-pipeline python=3.11 -y
conda activate health-pipeline
```

You should see `(health-pipeline)` at the start of your terminal prompt.

### Step 3 — Set up Google Cloud credentials

**3a. Create a GCP Project**
1. Go to [console.cloud.google.com](https://console.cloud.google.com)
2. Click the project dropdown → **New Project**
3. Name it `health-data-pipeline` → **Create**

**3b. Enable APIs**
1. Go to **APIs & Services → Library**
2. Search for and enable **Google Sheets API**
3. Search for and enable **Google Drive API**

**3c. Create a Service Account**
1. Go to **APIs & Services → Credentials**
2. Click **Create Credentials → Service Account**
3. Name it `health-pipeline-sa` → **Create and Continue**
4. Skip optional steps → **Done**

**3d. Download the JSON Key**
1. Click on your service account → **Keys** tab
2. **Add Key → Create New Key → JSON → Create**
3. Move the downloaded file to `credentials/`
4. Rename it to `service_account.json`

**3e. Share the Google Sheet**
1. Open `credentials/service_account.json`
2. Copy the `client_email` value
3. Open your Google Sheet → **Share**
4. Paste the email → **Viewer** → **Send**

### Step 4 — Configure environment variables

Create a `.env` file in the project root:

```bash
# GCP
GCP_SERVICE_ACCOUNT_PATH=./credentials/service_account.json
GOOGLE_SHEET_NAME=patient_data

# Snowflake
SNOWFLAKE_ACCOUNT=your-account-identifier
SNOWFLAKE_USER=your-username
SNOWFLAKE_PASSWORD=your-password
SNOWFLAKE_ROLE=SYSADMIN
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
SNOWFLAKE_DATABASE_DEV=HEALTH_DB_DEV
SNOWFLAKE_DATABASE_PROD=HEALTH_DB_PROD
```

> Never commit `.env` or `credentials/` to GitHub — both are in `.gitignore`.

### Step 5 — Set up dbt profiles

Create `profiles.yml` inside `dbt_project/health_pipeline/`:

```yaml
health_pipeline:
  target: dev
  outputs:
    dev:
      type: snowflake
      account: your-account-identifier
      user: your-username
      password: your-password
      role: SYSADMIN
      warehouse: COMPUTE_WH
      database: HEALTH_DB_DEV
      schema: public
      threads: 4

    prod:
      type: snowflake
      account: your-account-identifier
      user: your-username
      password: your-password
      role: SYSADMIN
      warehouse: COMPUTE_WH
      database: HEALTH_DB_PROD
      schema: public
      threads: 4
```

> `profiles.yml` is git ignored — never commit it.

### Step 6 — Set up Prefect Cloud

```bash
prefect cloud login
```

Follow the browser prompts to authenticate.

### Step 7 — Run setup

```bash
python scripts/setup.py
```

This single command:
1. Loads environment variables
2. Installs all Python dependencies
3. Creates Snowflake databases and schemas (`HEALTH_DB_DEV` and `HEALTH_DB_PROD`) # Prod DB not used in this exercise
4. Installs dbt packages

---

## Running the Pipeline, either run full pipeline or run individual steps

### 1) Run the full pipeline

```bash
python orchestration/pipeline_flow.py
```

This runs all steps in order:
1. Load environment variables
2. SQLFluff lint — checks SQL code quality
3. Ingest from Google Sheet → `raw.raw_raw_patient`
4. dbt run → `uat.uat_patient` → `consumption.consumption_fhir_patient`
5. dbt test → FHIR R4 data quality checks

### 2) Run individual steps

**Ingestion only:**
```bash
python ingestion/load_data.py dev
```

**SQL linting only:** (optional but recommended)
```bash
cd dbt_project/health_pipeline
sqlfluff lint models/ --dialect snowflake
```

**dbt models only:**
```bash
cd dbt_project/health_pipeline
dbt run --target dev
```

**dbt tests only:**
```bash
cd dbt_project/health_pipeline
dbt test --target dev
```

**View lineage and documentation:** (opens a local html page for lineage view)
```bash
cd dbt_project/health_pipeline
dbt docs generate
dbt docs serve 
```

---

## Verify the Pipeline

Check row counts in Snowflake:

```bash
snow sql -q "SELECT COUNT(*) FROM HEALTH_DB_DEV.raw.raw_raw_patient;" --connection health-pipeline
snow sql -q "SELECT COUNT(*) FROM HEALTH_DB_DEV.uat.uat_patient;" --connection health-pipeline
snow sql -q "SELECT COUNT(*) FROM HEALTH_DB_DEV.consumption.consumption_fhir_patient;" --connection health-pipeline
```

Check FHIR output:

```bash
snow sql -q "SELECT id, full_name, birth_date, gender, address, telecom, marital_status, insurance_number, nationality FROM HEALTH_DB_DEV.consumption.consumption_fhir_patient LIMIT 5;" --connection health-pipeline
```

You can also check this from Snowflake (Database explorer):
<img width="1778" height="689" alt="image" src="https://github.com/user-attachments/assets/9011a950-4297-4311-9db9-ec3508201069" />


---
---

## Scheduled Runs

The pipeline is deployed to **Prefect Cloud** and runs every **Monday at 06:00 UTC** (can be changed)

Trigger a manual run:
```bash
prefect deployment run 'health-data-pipeline/health-pipeline-deployment'
```

Monitor runs based on the link provided:
<img width="811" height="571" alt="image" src="https://github.com/user-attachments/assets/58f2d64e-58b3-4175-ab3e-a2f76f31fdee" />



---

## Production Considerations

This project is designed with production-grade patterns. For a full production deployment:

| Concern | Current | Production |
|---|---|---|
| Secrets | `profiles.yml` + `.env` | AWS Secrets Manager / GCP Secret Manager / Data Vaults |
| Pipeline execution | Local machine | AWS EC2 / Cloud Run |
| Data observability | dbt tests | Elementary Data / Monte Carlo |
| CI/CD | Manual deploy | GitHub Actions/DataOps |
| Data access control | Column masking + hashing | Immuta / AWS Lake Formation |
| Pre-commit checks | None | SQLFluff + yamllint + dbt-bouncer |
| Unit testing | None | pytest for Prefect tasks |

---

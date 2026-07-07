'''
    1. Extract data from google sheet using service account
    2. Connect to Snowflake and load into RAW table
    3. Log completed pipeline

'''

import gspread
import pandas as pd
import snowflake.connector
from google.oauth2.service_account import Credentials
from datetime import datetime, timezone
from dotenv import load_dotenv
import os
import logging
import json

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv(override=True)

# Constants
SCOPES = [
    'https://spreadsheets.google.com/feeds',
    'https://www.googleapis.com/auth/drive'
]


def get_google_sheet(sheet_name: str, worksheet_name: str = None):
    """Extract data from a Google Sheet (optionally a specific tab) and return as DataFrame and sheet ID."""
    try:
        logger.info(f"Connecting to Google Sheet: {sheet_name}" + (f" (tab: {worksheet_name})" if worksheet_name else ""))

        creds = Credentials.from_service_account_file(
            os.getenv('GCP_SERVICE_ACCOUNT_PATH'),
            scopes=SCOPES
        )

        client = gspread.authorize(creds)
        spreadsheet = client.open(sheet_name)
        sheet_id = spreadsheet.id

        worksheet = spreadsheet.worksheet(worksheet_name) if worksheet_name else spreadsheet.sheet1
        df = pd.DataFrame(worksheet.get_all_records())

        logger.info(f"Successfully extracted {len(df)} rows from Google Sheet")
        return df, sheet_id

    except Exception as e:
        logger.error(f"Failed to extract data from Google Sheet: {e}")
        raise


def get_snowflake_connection(database: str = None):
    """Create Snowflake connection from environment variables."""
    return snowflake.connector.connect(
        account=os.getenv('SNOWFLAKE_ACCOUNT'),
        user=os.getenv('SNOWFLAKE_USER'),
        password=os.getenv('SNOWFLAKE_PASSWORD'),
        role=os.getenv('SNOWFLAKE_ROLE'),
        warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
        database=database or os.getenv('SNOWFLAKE_DATABASE_DEV')
    )



def load_to_raw(df: pd.DataFrame, conn, sheet_id: str) -> None:
    """Load Google Sheet data into raw.patient as JSON."""
    try:
        logger.info("Loading data to raw.patient...")

        loaded_at = datetime.now(timezone.utc).isoformat()
        cursor = conn.cursor()

        cursor.execute("TRUNCATE TABLE raw.patient")

        def serialize_row(row):
            """Convert row to JSON safe dict."""
            result = {}
            for key, value in row.items():
                if pd.isna(value) if not isinstance(value, str) else value == '':
                    result[key] = None
                elif hasattr(value, 'isoformat'):
                    result[key] = value.isoformat()
                else:
                    result[key] = value
            return result

        count = 0
        for _, row in df.iterrows():
            raw_json = json.dumps(serialize_row(row))
            cursor.execute(
                "INSERT INTO raw.patient (loaded_at, source_sheet_id, raw_data) "
                "SELECT %s, %s, PARSE_JSON(%s)",
                (loaded_at, sheet_id, raw_json)
            )
            count += 1

        conn.commit()
        cursor.close()

        logger.info(f"Successfully loaded {count} rows into raw.patient")

    except Exception as e:
        logger.error(f"Failed to load data to raw: {e}")
        raise


def run_pipeline(target: str = 'dev'):
    """Main pipeline function."""
    logger.info(f"Starting ingestion pipeline for target: {target}")

    database = (
        os.getenv('SNOWFLAKE_DATABASE_DEV')
        if target == 'dev'
        else os.getenv('SNOWFLAKE_DATABASE_PROD')
    )

    # Extract
    df, sheet_id = get_google_sheet(os.getenv('GOOGLE_SHEET_NAME'), worksheet_name='patient')

    # Connect to Snowflake
    conn = get_snowflake_connection(database=database)

    # Load to raw
    load_to_raw(df, conn, sheet_id)

    conn.close()
    logger.info("Ingestion pipeline completed successfully")


if __name__ == "__main__":
    import sys
    target = sys.argv[1] if len(sys.argv) > 1 else 'dev'
    run_pipeline(target=target)
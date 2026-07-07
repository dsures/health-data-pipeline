'''
    1. Extract visit data from the 'visits' Google Sheet using the service account
    2. Connect to Snowflake and load into raw.visit
    3. Log completed pipeline
'''

import json
import logging
import os
from datetime import datetime, timezone

from dotenv import load_dotenv

from load_patient import get_google_sheet, get_snowflake_connection   

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

load_dotenv(override=True)


def load_to_raw_visit(df, conn, sheet_id: str) -> None:
    """Load Google Sheet visit data into raw.visit as JSON."""
    try:
        logger.info("Loading data to raw.visit...")

        loaded_at = datetime.now(timezone.utc).isoformat()
        cursor = conn.cursor()

        cursor.execute("TRUNCATE TABLE raw.visit")

        def serialize_row(row):
            result = {}
            for key, value in row.items():
                if hasattr(value, 'isoformat'):
                    result[key] = value.isoformat()
                else:
                    result[key] = value
            return result

        count = 0
        for _, row in df.iterrows():
            raw_json = json.dumps(serialize_row(row))
            cursor.execute(
                "INSERT INTO raw.visit (loaded_at, source_sheet_id, raw_data) "
                "SELECT %s, %s, PARSE_JSON(%s)",
                (loaded_at, sheet_id, raw_json)
            )
            count += 1

        conn.commit()
        cursor.close()

        logger.info(f"Successfully loaded {count} rows into raw.visit")

    except Exception as e:
        logger.error(f"Failed to load data to raw: {e}")
        raise


def run_pipeline(target: str = 'dev'):
    logger.info(f"Starting visit ingestion pipeline for target: {target}")

    database = (
        os.getenv('SNOWFLAKE_DATABASE_DEV')
        if target == 'dev'
        else os.getenv('SNOWFLAKE_DATABASE_PROD')
    )

    df, sheet_id = get_google_sheet(os.getenv('GOOGLE_SHEET_NAME'), worksheet_name='visits')

    conn = get_snowflake_connection(database=database)
    load_to_raw_visit(df, conn, sheet_id)
    conn.close()

    logger.info("Visit ingestion pipeline completed successfully")


if __name__ == "__main__":
    import sys
    target = sys.argv[1] if len(sys.argv) > 1 else 'dev'
    run_pipeline(target=target)
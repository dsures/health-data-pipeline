'''
    1. Create database and schemas for both dev and prod environments.
    2. Create raw table in the raw schema with 3 columns: loaded_at,
         source_sheet_id, raw_data.
    3. Use environment variables for database credentials and names.

'''


import snowflake.connector
from dotenv import load_dotenv
import os
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

load_dotenv()


def init_database(database: str) -> None:
    """Create database and all schemas for a given target."""
    logger.info(f"Initialising database: {database}")

    conn = snowflake.connector.connect(
        account=os.getenv('SNOWFLAKE_ACCOUNT'),
        user=os.getenv('SNOWFLAKE_USER'),
        password=os.getenv('SNOWFLAKE_PASSWORD'),
        role=os.getenv('SNOWFLAKE_ROLE'),
        warehouse=os.getenv('SNOWFLAKE_WAREHOUSE')
    )

    cursor = conn.cursor()

    try:
        # Create database
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {database}")
        logger.info(f"Database {database} created")

        # Create schemas
        schemas = ['raw', 'uat', 'consumption']
        for schema in schemas:
            cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {database}.{schema}")
            logger.info(f"Schema {database}.{schema} created")

        # Create raw table for patients
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {database}.raw.patient (
                loaded_at           TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP(),
                source_sheet_id     VARCHAR(255),
                raw_data            VARIANT
            )
        """)
        logger.info(f"Table {database}.raw.patient created")

        # Create raw table for visits
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {database}.raw.visit (
                loaded_at           TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP(),
                source_sheet_id     VARCHAR(255),
                raw_data            VARIANT
            )
        """)
        logger.info(f"Table {database}.raw.visit created")

        conn.commit()
        logger.info(f"Successfully initialised {database}")

    except Exception as e:
        logger.error(f"Failed to initialise {database}: {e}")
        raise

    finally:
        cursor.close()
        conn.close()


def run():
    """Initialise both dev and prod databases."""
    dev_db = os.getenv('SNOWFLAKE_DATABASE_DEV')
    prod_db = os.getenv('SNOWFLAKE_DATABASE_PROD')

    logger.info("Starting database initialisation...")

    init_database(dev_db)
    init_database(prod_db)

    logger.info("All databases initialised successfully")


if __name__ == "__main__":
    run()
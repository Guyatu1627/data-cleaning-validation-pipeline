import pandas as pd
from sqlalchemy import create_engine, text
import logging
import argparse
import sys
from pathlib import Path
import os
import io

# Fix UTF-8 encoding for Windows console
sys.stdout.reconfigure(encoding='utf-8')

# Clean, professional logging (Windows-safe)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('etl_pipeline.log', encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

def connect_db():
    try:
        db_user = os.getenv('DB_USER', 'etl_user')
        db_pass = os.getenv('DB_PASS', 'etl_pass')
        db_host = os.getenv('DB_HOST', 'localhost')
        db_name = os.getenv('DB_NAME', 'etl_demo')
        engine = create_engine(f'postgresql+psycopg2://{db_user}:{db_pass}@{db_host}/{db_name}')
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        logger.info("Database connection successful.")
        return engine
    except Exception as e:
        logger.error(f"Database connection failed: {e}")
        sys.exit(1)

def load_and_prepare_passengers(csv_path):
    try:
        df = pd.read_csv(csv_path)
        logger.info(f"Loaded {len(df)} rows from {csv_path}")

        # Map standard Kaggle Titanic columns to our schema
        column_map = {
            'PassengerId': 'passenger_id',
            'Name': 'name',
            'Sex': 'sex',
            'Age': 'age',
            'SibSp': 'sibsp',
            'Parch': 'parch',
            'Fare': 'fare',
            'Embarked': 'embarked'
        }
        df = df.rename(columns=column_map)
        df = df[[v for v in column_map.values() if v in df.columns]]

        missing_ages = df['age'].isna().sum()
        logger.info(f"Data prepared: {len(df)} passengers, {missing_ages} missing ages (will be imputed)")
        return df
    except Exception as e:
        logger.error(f"Error reading or preparing passengers CSV: {e}")
        sys.exit(1)

def create_tickets_df(passengers_df):
    return pd.DataFrame({
        'ticket_id': passengers_df['passenger_id'],
        'passenger_id': passengers_df['passenger_id'],
        'ticket': ['T' + str(i) for i in range(1, len(passengers_df)+1)],
        'class': 3,  # placeholder
        'cabin': 'Unknown'
    })

def load_to_staging(engine, passengers_df, tickets_df):
    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE staging_passengers, staging_tickets RESTART IDENTITY;"))
        passengers_df.to_sql('staging_passengers', conn, if_exists='append', index=False, method='multi')
        tickets_df.to_sql('staging_tickets', conn, if_exists='append', index=False, method='multi')
    logger.info("Data loaded to staging tables.")

def transform_and_load(engine):
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM tickets; DELETE FROM passengers;"))

        conn.execute(text("""
            INSERT INTO passengers (passenger_id, name, sex, age, family_size, embarked)
            SELECT 
                passenger_id,
                TRIM(name),
                LOWER(sex),
                COALESCE(age, 30.0),
                COALESCE(sibsp, 0) + COALESCE(parch, 0) + 1,
                COALESCE(embarked, 'S')
            FROM staging_passengers
            ON CONFLICT (passenger_id) DO UPDATE SET
                name = EXCLUDED.name, sex = EXCLUDED.sex, age = EXCLUDED.age,
                family_size = EXCLUDED.family_size, embarked = EXCLUDED.embarked;
        """))

        conn.execute(text("""
            INSERT INTO tickets (ticket_id, passenger_id, ticket_number, class, cabin)
            SELECT ticket_id, passenger_id, ticket, class, cabin
            FROM staging_tickets
            ON CONFLICT (ticket_id) DO UPDATE SET
                passenger_id = EXCLUDED.passenger_id, ticket_number = EXCLUDED.ticket_number,
                class = EXCLUDED.class, cabin = EXCLUDED.cabin;
        """))

    logger.info("Transformation complete – data in analytics tables.")

def show_analytics(engine):
    with engine.connect() as conn:
        result = conn.execute(text("SELECT * FROM analytics_summary ORDER BY passenger_count DESC;")).fetchall()
        logger.info("ANALYTICS RESULTS:")
        logger.info("Sex    | Port | Avg Age | Count")
        for row in result:
            logger.info(f"{row.sex:<6} | {row.embarked:<4} | {row.avg_age:>7} | {row.passenger_count:>5}")

def main(passengers_csv, tickets_csv=None):
    logger.info("🚀 Starting Advanced SQL ETL Pipeline (CSV → Postgres → Analytics)")
    passengers_path = Path(passengers_csv)
    if not passengers_path.exists():
        logger.error(f"❌ Passengers file not found: {passengers_csv}")
        sys.exit(1)

    engine = connect_db()
    passengers_df = load_and_prepare_passengers(passengers_path)
    if tickets_csv:
        tickets_path = Path(tickets_csv)
        if not tickets_path.exists():
            logger.error(f"❌ Tickets file not found: {tickets_csv}")
            sys.exit(1)
        tickets_df = load_and_prepare_tickets(tickets_path)
    else:
        tickets_df = create_tickets_df(passengers_df)
    load_to_staging(engine, passengers_df, tickets_df)
    transform_and_load(engine)
    show_analytics(engine)
    logger.info("✅ PROJECT 2 ETL PIPELINE COMPLETED SUCCESSFULLY!")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--passengers', required=True, help='Path to passengers CSV file')
    parser.add_argument('--tickets', required=False, help='Path to tickets CSV file (optional, will create dummy if not provided)')
    args = parser.parse_args()
    main(args.passengers, args.tickets)
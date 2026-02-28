import os
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
load_dotenv()
# -----------------------------
# 🔧 Configuration
# -----------------------------
# Database connection details (update as needed)
DB_HOST = os.getenv("MR_DB_HOST")
DB_PORT = 3306
DB_NAME = os.getenv("MR_DB_NAME")
DB_USER = os.getenv("MR_DB_USER")
DB_PASSWORD = os.getenv("MR_DB_PASSWORD")

# Tables to extract
TABLES = ['customer', 'film', 'rental', 'payment']

# Output folder
OUTPUT_DIR = 'data_extract'


# -----------------------------
# 🔌 Connect to MariaDB
# -----------------------------
def create_db_engine():
    """Create a SQLAlchemy engine for MariaDB."""
    url = f"mariadb+mariadbconnector://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    return create_engine(url)


# -----------------------------
# 📤 Extract and Save
# -----------------------------
def extract_and_save_table(engine, table_name, output_folder):
    """Extracts a table and saves it as a compressed CSV (.csv.gz)."""
    print(f"Extracting `{table_name}`...")
    df = pd.read_sql(f"SELECT * FROM {table_name}", engine)

    # Ensure output folder exists
    os.makedirs(output_folder, exist_ok=True)

    # Save to compressed file
    output_path = os.path.join(output_folder, f"{table_name}.csv.gz")
    df.to_csv(output_path, index=False, compression='gzip')
    print(f"Saved to {output_path}")


# -----------------------------
# 🚀 Main ETL Extract Function
# -----------------------------
def main():
    engine = create_db_engine()
    for table in TABLES:
        extract_and_save_table(engine, table, OUTPUT_DIR)


if __name__ == "__main__":
    main()

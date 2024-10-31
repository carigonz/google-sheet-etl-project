import pandas as pd
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.engine import Engine
import os

from utils.db import create_postgres_connection
from utils.constants import DB_USER, TEMP_DIR


def load_data(**context) -> None:
    """
    Load transformed data from parquet files into PostgreSQL database tables.

    This function reads the transformed DataFrames from parquet files, establishes a database connection,
    and loads the data into two tables: 'devolutions' and 'pdfs'. After loading, it cleans up temporary files.

    Args:
        **context: Airflow context dictionary containing task instance and other execution info

    Raises:
        SQLAlchemyError: If there is an error connecting to or writing to the database
        Exception: If any other error occurs during execution
    """
    ti = context['ti']
    file_paths = ti.xcom_pull(task_ids='transform_data')
    transformed_df_path, transformed_tables_path = file_paths

    df = pd.read_parquet(transformed_df_path)
    df_tables = pd.read_parquet(transformed_tables_path)

    engine = None
    try:
        engine: Engine = create_postgres_connection()

        with engine.connect() as connection:
            print('Connection created')

            df.to_sql(
                'devolutions',
                connection,
                schema=f"{DB_USER}_schema",
                if_exists="append",
                index=False,
            )
            print('Devolutions data loaded')

            df_tables.to_sql(
                'pdf_devolutions',
                connection,
                schema=f"{DB_USER}_schema",
                if_exists="append",
                index=False,
            )
            print('PDFs data loaded')

    except SQLAlchemyError as e:
        print(f"Database error: {e}")
        raise
    except Exception as e:
        print(f"An error occurred: {e}")
        raise
    finally:
        if engine:
            engine.dispose()

    __clean_temp_files()


def __clean_temp_files() -> None:
    """
    Clean all files from the temporary directory.
    """
    if os.path.exists(TEMP_DIR):
        files = os.listdir(TEMP_DIR)
        for file_name in files:
            file_path = os.path.join(TEMP_DIR, file_name)
            try:
                if os.path.isfile(file_path):
                    os.remove(file_path)
                    print(f"Removed file: {file_path}")
            except Exception as e:
                print(f"Error removing {file_path}: {e}")

import pandas as pd
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.engine import Engine

from utils.db import create_postgres_connection
# from utils.constants import DB_USER

DB_USER = '2024_carolina_gonzalez'


def load_data(table_name_devolutions: pd.DataFrame, table_name_pdf: str, **kwargs: any) -> None:
    """
    Load data from a pandas DataFrame into the specified PostgreSQL table.

    Args:
        data (pd.DataFrame): Data to be inserted.
        table_name (str): Name of the table to insert the data into.
    """
    engine = None
    try:
        ti = kwargs['ti']
        df, df_tables = ti.xcom_pull(task_ids='transform_initial_data')

        print('data extracted')
        print(f"Devolutions data shape: {df.shape}")
        print(f"PDF tables data shape: {df_tables.shape}")

        engine: Engine = create_postgres_connection()
        print('Engine created')

        with engine.connect() as connection:
            print('Connection created')
            df = df.reset_index()
            df.to_sql(
                table_name_devolutions,
                connection,
                schema=f"{DB_USER}_schema",
                if_exists="append",
                index=False,
            )
            print('Devolutions data loaded')

            # Reset the index to make it a column
            df_tables = df_tables.reset_index()

            df_tables.to_sql(
                table_name_pdf,
                connection,
                schema=f"{DB_USER}_schema",
                if_exists="append",
                index=False,
            )
            print('PDF tables data loaded')
        print('Data loaded successfully')

    except SQLAlchemyError as e:
        print(f"Database error: {e}")
        raise
    except Exception as e:
        print(f"An error occurred: {e}")
        raise
    finally:
        if engine:
            engine.dispose()

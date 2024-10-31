from datetime import datetime
import pandas as pd
from utils.constants import PDF_COLUMN, TEMP_DIR


def transform_data(**context) -> tuple[str, str]:
    """
    Transform extracted data by mapping columns, removing unused ones, and converting data types.

    This function takes the parquet files created by extract_data, performs several transformations
    on both the main DataFrame and PDF tables DataFrame, and saves the transformed data back to
    parquet files.

    Args:
        **context: Airflow context dictionary containing task instance and other execution info

    Returns:
        tuple[str, str]: A tuple containing the file paths of the transformed parquet files
            (transformed_df_path, transformed_tables_path)
    """
    ti = context['ti']
    df_path, df_tables_path = ti.xcom_pull(task_ids='extract_data')

    df = pd.read_parquet(df_path)
    df_tables = pd.read_parquet(df_tables_path)

    df, df_tables = map_custom_columns(df, df_tables)

    df, df_tables = remove_unused_columns(df, df_tables)

    df, df_tables = convert_data_types(df, df_tables)

    # Save transformed DataFrames to parquet files in same dir
    transformed_df_path = f'{TEMP_DIR}/transformed_df.parquet'
    transformed_tables_path = f'{TEMP_DIR}/transformed_tables.parquet'

    df.to_parquet(transformed_df_path)
    df_tables.to_parquet(transformed_tables_path)

    return transformed_df_path, transformed_tables_path


def convert_data_types(df: pd.DataFrame, df_tables: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Convert data types of columns in both main DataFrame and PDF tables DataFrame.

    Args:
        df (pd.DataFrame): The main DataFrame.
        df_tables (pd.DataFrame): The PDF tables DataFrame.

    Returns:
        tuple[pd.DataFrame, pd.DataFrame]: A tuple containing the DataFrames with converted data types.
    """
    # First map original dataset keys
    df['original_timestamp'] = pd.to_datetime(df['original_timestamp'])
    df['note_date'] = pd.to_datetime(df['note_date'], format='%d/%m/%Y')
    df['note_number'] = df['note_number'].astype(int)
    df['note_amount'] = df['note_amount'].astype(int)
    df['should_be_paid'] = df['should_be_paid'].map({'SI': True, 'TEST': True, 'NO': False, '': False})
    df['was_uploaded'] = df['was_uploaded'].map({'SI': True, 'TEST': True, 'NO': False, '': False})
    df['month'] = df['month'].astype(int)
    df['year'] = df['year'].fillna(2024).replace('', 2024).astype(int)

    # clean data from pdf tables
    df_tables['description'] = df_tables['description'].str.replace('\n', ' ').str.strip()
    df_tables['quantity'] = df_tables['quantity'].str.replace(',', '.')\
        .str.extract('(\d+\.?\d*)').astype(float)  # noqa: W605
    if 'total_amount' in df_tables.columns and df_tables['total_amount'].notna().any():
        df_tables['total_amount'] = df_tables['total_amount']\
            .str.replace('$', '', regex=False)\
            .str.replace('.', '', regex=False).str.replace(',', '.', regex=False)
    if 'devolution_type' in df_tables.columns and df_tables['devolution_type'].notna().any():
        df_tables['devolution_type'] = df_tables['devolution_type'].str.replace('\n', ' ').str.strip()

    if 'pvp' in df_tables.columns and df_tables['pvp'].notna().any():
        df_tables['pvp'] = df_tables['pvp'].str.replace('$', '', regex=False)\
            .str.replace('.', '', regex=False).str.replace(',', '.', regex=False)

    return df, df_tables


def remove_unused_columns(df: pd.DataFrame, df_tables: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Remove unused columns from the DataFrame.

    Args:
        df (pd.DataFrame): The input DataFrame.

    Returns:
        pd.DataFrame: The DataFrame with unused columns removed.
    """
    df = df.drop(columns=['not_used_date', 'not_used_column'])
    df_tables = df_tables[(df_tables['quantity'].notna()) & (df_tables['quantity'] != 0)].reset_index(drop=True)
    if 'not_used_column' in df_tables.columns:
        df_tables = df_tables.drop(columns=['not_used_column'])

    return df, df_tables


def map_custom_columns(df: pd.DataFrame, df_tables: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Map custom column names for both main DataFrame and PDF tables DataFrame.

    Args:
        df (pd.DataFrame): The main DataFrame.
        df_tables (pd.DataFrame): The PDF tables DataFrame.

    Returns:
        tuple[pd.DataFrame, pd.DataFrame]: A tuple containing the DataFrames with mapped column names.
    """

    column_mapping_pdf = {
        'Código': 'code',
        'Descripción': 'description',
        'PVP': 'pvp',
        'Cantidad': 'quantity',
        'Total': 'total_amount',
        'Articulo en\nfalta': 'not_used_column',
        'Causa de\ndevolucion': 'devolution_type',
        'Causa de\ndevolución': 'devolution_type_gd',
        'Incluido\nAlbaran': 'not_used_column',
        'ComCalid': 'not_used_column'
    }
    df_tables = __map_columns_to_tables(df=df_tables, column_mapping=column_mapping_pdf)
    df_tables['extracted_date'] = datetime.today()

    column_mapping_devolution = {
        'Marca temporal': 'original_timestamp',
        'FAMILIA PRODUCTOS': 'product_family',
        'FECHA NOTA': 'note_date',
        'NOTA': 'note_number',
        'MONTO': 'note_amount',
        'RECONOCIMIENTO': 'should_be_paid',
        'USUARIO': 'user',
        'PDF NOTA': PDF_COLUMN,
        'OSERVACIONES': 'additional_info',
        'FECHA': 'not_used_date',
        'IDDEVOLUCION': 'not_used_column',
        'DETALLES JT': 'details_jt',
        'FORM PC': 'was_uploaded',
        'MES': 'month',
        'ANO': 'year',
        'MES CONFIRMADA': 'confirmed_month'
    }
    df = __map_columns_to_tables(df, column_mapping=column_mapping_devolution)
    df['extracted_date'] = datetime.today()

    return df, df_tables


def __map_columns_to_tables(df: pd.DataFrame, column_mapping: dict) -> pd.DataFrame:
    """
    Map columns of a DataFrame according to the provided column mapping.

    Args:
        df (pd.DataFrame): The input DataFrame.
        column_mapping (dict): A dictionary mapping old column names to new column names.

    Returns:
        pd.DataFrame: The DataFrame with renamed columns.
    """
    return df.rename(columns=column_mapping)

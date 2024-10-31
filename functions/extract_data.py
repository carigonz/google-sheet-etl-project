import os

from utils.google_drive import get_google_sheet_data, make_df_from_pdfs
from utils.constants import SHEET_NAME, WORKSHEET_NAME, PDF_COLUMN, NOTE_COLUMN, TEMP_DIR
import pandas as pd


def extract_data(processing_dates: str, **context) -> tuple[str, str]:
    """
    Extract data for multiple dates

    Args:
        processing_dates (List[str]): List of dates to process in DD/MM/YYYY format
    """
    dates_list = processing_dates.split(',')
    dates_list = [date.strip() for date in dates_list]
    dfs = []
    dfs_tables = []

    for date in dates_list:
        try:
            df, df_tables = __extract_single_date(date)
            dfs.append(df)
            dfs_tables.append(df_tables)
        except Exception as e:
            print(f"Error processing date {date}: {e}")
            continue

    if not dfs or not dfs_tables:
        raise ValueError("No data was extracted for any of the provided dates")

    final_df = pd.concat(dfs, ignore_index=True)
    final_df_tables = pd.concat(dfs_tables, ignore_index=True)

    return __make_parquet_files(final_df, final_df_tables)


def __extract_single_date(date: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Extract data for a single date
    """
    df: pd.DataFrame = get_google_sheet_data(SHEET_NAME, WORKSHEET_NAME, date)

    new_df = df[[PDF_COLUMN, NOTE_COLUMN]]

    df_tables = make_df_from_pdfs(new_df)

    return df, df_tables


def __create_temp_dir():
    """
    Create a temporary directory for storing parquet files.

    Returns:
        str: Path to the created temporary directory
    """
    os.makedirs(TEMP_DIR, exist_ok=True)
    return TEMP_DIR


def __make_parquet_files(df: pd.DataFrame, df_tables: pd.DataFrame) -> tuple[str, str]:
    """
    Save DataFrames as parquet files in the temporary directory.

    Args:
        df (pd.DataFrame): Main DataFrame to save
        df_tables (pd.DataFrame): PDF tables DataFrame to save

    Returns:
        tuple[str, str]: Tuple containing paths to the saved parquet files:
            - Path to main DataFrame parquet file
            - Path to PDF tables DataFrame parquet file
    """
    # This is to avoid errors when converting to parquet
    numeric_columns = ['year', 'month']
    for col in numeric_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype('int64')

    __create_temp_dir()
    df_path = f'{TEMP_DIR}/df_devolutions.parquet'
    df_tables_path = f'{TEMP_DIR}/df_tables.parquet'
    df.to_parquet(df_path)
    df_tables.to_parquet(df_tables_path)
    return df_path, df_tables_path

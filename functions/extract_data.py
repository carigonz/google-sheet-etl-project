from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import os

from utils.google_drive import get_google_sheet_data, make_df_from_pdfs
from utils.constants import SHEET_NAME, WORKSHEET_NAME, PDF_COLUMN, NOTE_COLUMN, TEMP_DIR
import pandas as pd


def extract_data(**kwargs: any) -> tuple[str, str] | None:
    """
    Extract data from Google Sheets and PDF files, saving results as parquet files.

    This function retrieves data from a Google Sheet for a specific date (either provided or yesterday),
    extracts information from associated PDF files, and saves both datasets as parquet files.

    Args:
        **kwargs: Keyword arguments
            custom_date (str, optional): Date in format 'DD/MM/YYYY' to extract data for.
            If not provided, defaults to yesterday.

    Returns:
        tuple[str, str] | None: A tuple containing paths to the saved parquet files:
            - Path to main DataFrame parquet file
            - Path to PDF tables DataFrame parquet file
            Returns None if extraction fails
    """
    custom_date = kwargs.get('custom_date')
    if custom_date is None:
        yesterday = datetime.now(ZoneInfo("UTC")) - timedelta(days=1)
        custom_date = yesterday.strftime('%d/%m/%Y')

    df: pd.DataFrame = get_google_sheet_data(SHEET_NAME, WORKSHEET_NAME, custom_date)

    new_df = df[[PDF_COLUMN, NOTE_COLUMN]]

    df_tables = make_df_from_pdfs(new_df)

    return __make_parquet_files(df, df_tables)


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
    __create_temp_dir()
    df_path = f'{TEMP_DIR}/df_devolutions.parquet'
    df_tables_path = f'{TEMP_DIR}/df_tables.parquet'
    df.to_parquet(df_path)
    df_tables.to_parquet(df_tables_path)
    return df_path, df_tables_path

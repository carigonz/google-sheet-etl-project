from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from utils.google_drive import get_google_sheet_data, make_df_from_pdfs
from utils.constants import SHEET_NAME, WORKSHEET_NAME, PDF_COLUMN
import pandas as pd


def extract_data(**kwargs: any) -> tuple[pd.DataFrame, pd.DataFrame] | None:

    custom_date = kwargs.get('custom_date')
    if custom_date is None:
        yesterday = datetime.now(ZoneInfo("UTC")) - timedelta(days=1)
        custom_date = yesterday.strftime('%d/%m/%Y')

    df: pd.DataFrame = get_google_sheet_data(SHEET_NAME, WORKSHEET_NAME, custom_date)

    new_df = df[PDF_COLUMN].reset_index()

    df_tables = make_df_from_pdfs(new_df)

    return df, df_tables

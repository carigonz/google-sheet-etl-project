import json
import requests

from utils.google_drive import get_google_sheet_data, make_df_from_pdfs
from utils.constants import SHEET_NAME, WORKSHEET_NAME, PDF_COLUMN
import pandas as pd


def extract_initial_data(**kwargs: any) -> tuple[pd.DataFrame, pd.DataFrame] | None:

    df: pd.DataFrame = get_google_sheet_data(SHEET_NAME, WORKSHEET_NAME)

    print("Data loaded from Google Sheets into the DataFrame:")
    print(df)

    df_tables = []

    new_df = df[PDF_COLUMN].reset_index()

    print("new_df DataFrame:")
    print(new_df)

    df_tables = make_df_from_pdfs(new_df)

    return df, df_tables


def extract_data_from_api(api_name: str, **kwargs: any) -> dict | None:
    """
    Extracts data from different APIs based on the provided API name.

    Args:
        api_name (str): The name of the API to extract data from.
            Valid options are 'usdt', 'usd', or 'bcra'.
        **kwargs: Additional arguments that can be passed if necessary.

    Returns:
        dict | None: The response data from the API in JSON format if the request
        is successful.

    Raises:
        ValueError: If an invalid API name is provided.
        requests.exceptions.RequestException: If there is an issue with the API request.
    """
    api_urls = {
        "usdt": "https://criptoya.com/api/usdt/ars/100",
        "usd": "https://criptoya.com/api/dolar",
        "bcra": "https://api.bcra.gob.ar/estadisticas/v2.0/principalesvariables",
    }

    api_url: str | None = api_urls.get(api_name)

    if not api_url:
        raise ValueError(
            f"Invalid API name: {api_name}. Valid options are 'usdt', 'usd', or 'bcra'."
        )

    headers: dict[str, str] = {"Accept-Language": "en-US"} if api_name == "bcra" else {}

    response = requests.get(api_url, headers=headers, verify=False)
    response.raise_for_status()
    data: dict = json.dumps(response.json())
    return data

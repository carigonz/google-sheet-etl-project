import json
import requests

import requests
HISTORIC_DOLARS_URL = 'https://api.argentinadatos.com/v1/cotizaciones/dolares'

def extract_data_from_api() -> dict | None:
    """
    Extracts data from different APIs based on the provided API name.

    Returns:
        dict | None: The response data from the bcra API in JSON format

    Raises:
        requests.exceptions.RequestException: API request error.
    """
    api_url = "https://dolarapi.com/v1/dolares/blue"

    response = requests.get(api_url, headers={}, verify=False)
    response.raise_for_status()

    data: dict = json.dumps(response.json())
    return data
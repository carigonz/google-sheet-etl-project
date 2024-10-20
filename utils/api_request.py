import json
import requests


def extract_data_from_api(type: str) -> dict | None:
    """
    Extracts data from different APIs based on the provided API type.

    Args:
        type (str): The type of API to extract data from.
            Valid options are 'historic_dolar' or 'today_dolar'.

    Returns:
        dict | None: The response data from the API in JSON format.

    Raises:
        ValueError: If an invalid API type is provided.
        requests.exceptions.RequestException: If there is an issue with the API request.
    """
    urls_dict = {
        "historic_dolar": "https://api.argentinadatos.com/v1/cotizaciones/dolares",
        "today_dolar": "https://dolarapi.com/v1/dolares/blue",
    }

    api_url = urls_dict.get(type)

    if not api_url:
        raise ValueError(
            f"Invalid API name: {type}. Valid options are 'historic_dolar', 'today_dolar'."
        )

    response = requests.get(api_url, headers={}, verify=False)
    response.raise_for_status()

    data: dict = json.dumps(response.json())
    return data

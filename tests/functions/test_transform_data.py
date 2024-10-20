from datetime import datetime
import pandas as pd
import pytest

from functions.transform_data import convert_data_types


def test_convert_data_types(mock_transform_data_payload):
    """Test for the convert_data_types function."""
    df, df_tables = convert_data_types(mock_transform_data_payload)

    expected_columns = [
        "original_timestamp",
        "note_date",
        "note_number",
        "note_amount",
        "should_be_paid",
        "was_uploaded",
        "month",
        "year",
    ]
    assert list(df.columns) == expected_columns

    assert df["original_timestamp"][0] == datetime(2024, 6, 26, 13, 53, 43)
    assert df["note_date"][0] == datetime(2024, 6, 26)
    assert df["note_number"][0] == 101407
    assert df["note_amount"][0] == 37120
    assert df["should_be_paid"][0]
    assert df["was_uploaded"][0]
    assert df["month"][0] == 6
    assert df["year"][0] == 2024

    # Assertions for df_tables
    expected_columns_tables = [
        'code',
        'description',
        'pvp',
        'quantity',
        'total_amount',
        'devolution_type'
    ]

    assert list(df_tables.columns) == expected_columns_tables
    assert df_tables['description'][0] == 'Product A with newline'
    assert df_tables['pvp'][0] == 1234.56
    assert df_tables['quantity'][0] == 1.5
    assert df_tables['total_amount'][0] == 37120
    assert df_tables['devolution_type'][0] == 'CAUSA 3'
    assert df_tables['devolution_id'][0] == 0


''' FIXTURES '''


@pytest.fixture
def mock_transform_data_payload():
    # DataFrame for the main data
    df = pd.DataFrame({
        'Marca temporal': ['26/06/2024 13:53:43', '26/06/2024 13:53:43'],
        'FAMILIA PRODUCTOS': ['REFRIGERADO', 'REFRIGERADO'],
        'FECHA NOTA': ['26/06/2024', '26/06/2024'],
        'NOTA': ['101407', '101408'],
        'MONTO': ['37120', '643224'],
        'RECONOCIMIENTO': ['NO', 'NO'],
        'USUARIO': ['CANDELA', 'MARIO'],
        'PDF NOTA': ['123456', '789101'],
        'OSERVACIONES': ['', ''],
        'FECHA': ['', ''],
        'IDDEVOLUCION': ['', ''],
        'DETALLES JT': ['', ''],
        'FORM PC': ['', ''],
        'MES': ['8', '8'],
        'ANO': ['2024', '2024'],
        'MES CONFIRMADA': ['septiembre2024', 'septiembre2024']
    })

    # DataFrame for the table data from PDFs
    df_tables = pd.DataFrame({
        'Código': ['608', '123456', '789101', '608'],
        'Descripción': [
            'Product A\nwith newline',
            'Product B\nwith newline',
            'Product C\nwith newline',
            'Product D\nwith newline'
        ],
        'PVP': ['1.234,56', '1.234,56', '1.234,56', '1.234,56'],
        'Cantidad': ['1,5', '1,5', '1,5', '1,5'],
        'Total': ['37120', '643224', '37120', '643224'],
        'Causa de\ndevolucion': ['CAUSA 3', 'CAUSA 5', 'CAUSA 5', 'CAUSA 5'],
        'devolution_id': ['0', '1', '1', '1']
    })

    return df, df_tables

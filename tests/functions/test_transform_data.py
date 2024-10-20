import pandas as pd
import pytest

from functions.transform_data import convert_data_types, map_custom_columns


def test_convert_data_types(mock_transform_data_payload):
    df, df_tables = mock_transform_data_payload
    df, df_tables = map_custom_columns(df, df_tables)

    converted_df, converted_df_tables = convert_data_types(df, df_tables)

    assert converted_df['note_number'].iloc[0] == 101407

    assert isinstance(converted_df_tables['quantity'].iloc[0], float)
    assert isinstance(converted_df_tables['total_amount'].iloc[0], float)
    assert isinstance(converted_df_tables['pvp'].iloc[0], float)


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

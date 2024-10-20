import pandas as pd
from utils.constants import PDF_COLUMN


def transform_initial_data(**kwargs: any) -> tuple[pd.DataFrame, pd.DataFrame]:
    ti = kwargs['ti']
    df, df_tables = ti.xcom_pull(task_ids='extract_initial_data')

    df, df_tables = map_custom_columns(df, df_tables)

    df = remove_unused_columns(df)

    df, df_tables = convert_data_types(df, df_tables)

    print(df)
    print(df.shape)
    print("df columns:", df.columns)
    return df, df_tables


def convert_data_types(df: pd.DataFrame, df_tables: pd.DataFrame) -> pd.DataFrame:

    # First map original dataset keys
    df['original_timestamp'] = pd.to_datetime(df['original_timestamp'])
    df['note_date'] = pd.to_datetime(df['note_date'], format='%d/%m/%Y')
    df['note_number'] = df['note_number'].astype(int)
    df['note_amount'] = df['note_amount'].astype(int)
    df['should_be_paid'] = df['should_be_paid'].map({'SI': True, 'TEST': True, 'NO': False, '': False})
    df['was_uploaded'] = df['was_uploaded'].map({'SI': True, 'TEST': True, 'NO': False, '': False})
    df['month'] = df['month'].astype(int)
    df['year'] = df['year'].astype(int)

    # clean data from pdf tables
    df_tables['description'] = df_tables['description'].str.replace('\n', ' ').str.strip()
    df_tables['quantity'] = df_tables['quantity'].str.replace(',', '.')\
        .str.extract('(\d+\.?\d*)').astype(float)  # noqa: W605
    df_tables['total_amount'] = df_tables['total_amount'].str.replace('$', '', regex=False)\
        .str.replace('.', '', regex=False).str.replace(',', '.', regex=False).astype(float)
    df_tables['devolution_type'] = df_tables['devolution_type'].str.replace('\n', ' ').str.strip()
    df_tables['pvp'] = df_tables['pvp'].str.replace('$', '', regex=False)\
        .str.replace('.', '', regex=False).str.replace(',', '.', regex=False).astype(float)

    return df, df_tables


def remove_unused_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.drop(columns=['not_used_date', 'not_used_column'])
    return df


def map_custom_columns(df: pd.DataFrame, df_tables: pd.DataFrame) -> pd.DataFrame:
    column_mapping_pdf = {
        'Código': 'code',
        'Descripción': 'description',
        'PVP': 'pvp',
        'Cantidad': 'quantity',
        'Total': 'total_amount',
        'Causa de\ndevolucion': 'devolution_type'
    }
    df_tables = __map_columns_to_tables(df=df_tables, column_mapping=column_mapping_pdf)

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

    return df, df_tables


def __map_columns_to_tables(df: pd.DataFrame, column_mapping: dict) -> pd.DataFrame:
    return df.rename(columns=column_mapping)

import gspread
import pandas as pd
import requests
import pdfplumber
from io import BytesIO
import io
from oauth2client.service_account import ServiceAccountCredentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from google.oauth2.service_account import Credentials
from utils.constants import CREDENTIALS_FILE, SHEET_NAME, WORKSHEET_NAME, PDF_COLUMN


def get_google_sheet_data(sheet_name, worksheet_name, filter_column="FORM PC", filter_value='TEST'):
    """Access Google Sheet and retrieve data as a DataFrame

    Args:
        sheet_name (str): Name of the Google Sheets file
        worksheet_name (str): Name of the worksheet
        filter_column (str, optional): Column to filter by
        filter_value (Any, optional): Value to filter for

    Returns:
        pd.DataFrame: DataFrame containing the worksheet data
    """
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']

    credentials = ServiceAccountCredentials.from_json_keyfile_name(CREDENTIALS_FILE, scope)

    client = gspread.authorize(credentials)

    sheet = client.open(sheet_name)

    worksheet = sheet.worksheet(worksheet_name)

    data = worksheet.get_all_records()

    filtered_rows = data

    if filter_column and filter_value:
        filtered_rows = [row for row in data if row.get(filter_column) == filter_value]
        print(filtered_rows)
    else:
        filtered_rows = data[1:]
    # Create a dictionary to map original column names to new column names
    column_mapping = {
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
        'MES CONFIRMADA': 'confirmed_date'
    }

    # Create DataFrame with original column names
    df = pd.DataFrame(filtered_rows, columns=data[0])

    # Rename columns using the mapping
    df = df.rename(columns=column_mapping)

    # Select only the columns we want in the final DataFrame
    df = df[list(column_mapping.values())]
    return df


def download_pdf_from_url(pdf_url):
    """Downloads a PDF from a URL

    Args:
        pdf_url (str): URL of the PDF

    Returns:
        BytesIO: Downloaded PDF file as a BytesIO object
    """
    response = requests.get(pdf_url)
    if response.status_code == 200:
        return BytesIO(response.content)
    else:
        raise Exception(f"Error al descargar el PDF: {response.status_code}")


def extract_table_with_pdfplumber(pdf_file):
    """Extracts tables from a PDF using pdfplumber

    Args:
        pdf_file (BytesIO): PDF file as a BytesIO object

    Returns:
        List[pd.DataFrame]: List of DataFrames containing the extracted tables
    """
    tables_data = []

    column_mapping = {
        'Código': 'code',
        'Descripción': 'description',
        'PVP': 'pvp',
        'Cantidad': 'quantity',
        'Total': 'total_amount',
        'Causa de\ndevolucion': 'devolution_type'
    }
    try:
        with pdfplumber.open(pdf_file) as pdf:
            for page in pdf.pages:
                tables = page.extract_tables()
                for table in tables:
                    df = pd.DataFrame(table[1:], columns=table[0])
                    df = df.rename(columns=column_mapping)
                    tables_data.append(df)
        return tables_data
    except Exception as e:
        print(f"Error al procesar el PDF: {str(e)}")
        return None


def download_pdf_from_drive(file_id: str):
    """Downloads a file from Google Drive

    Args:
        df (pd.DataFrame): DataFrame containing the file ID

    Returns:
        io.BytesIO: Downloaded file
    """
    try:
        credentials = Credentials.from_service_account_file(CREDENTIALS_FILE,
                                                            scopes=['https://www.googleapis.com/auth/drive.readonly'])
    except Exception as e:
        raise ValueError("No credentials provided")

    drive_service = build('drive', 'v3', credentials=credentials)
    request = drive_service.files().get_media(fileId=file_id)
    file = io.BytesIO()
    downloader = MediaIoBaseDownload(file, request)
    done = False
    while done is False:
        status, done = downloader.next_chunk()
    return file


def extract_file_id_from_url(url):
    """Extracts the file ID from a Google Drive URL

    Args:
        url (str): Google Drive URL

    Returns:
        str: File ID
    """
    return url.split('=')[-1]


def get_original_data():
    df = get_google_sheet_data(SHEET_NAME, WORKSHEET_NAME)

    print("Data loaded from Google Sheets into the DataFrame:")
    print(df)

    return df


def make_df_from_pdfs(data: pd.DataFrame):
    all_tables = []

    for index, row in data.iterrows():
        pdf_url = row[PDF_COLUMN]

        file_id = extract_file_id_from_url(pdf_url)

        pdf_file = download_pdf_from_drive(file_id)

        tables = extract_table_with_pdfplumber(pdf_file)

        if tables:
            for table in tables:
                # add foreign key to original table
                table['devolution_id'] = index
                all_tables.append(table)
        else:
            print(f"No tables found in PDF for index {index}")

    if all_tables:
        # Combine all tables into a single DataFrame
        result_df = pd.concat(all_tables, ignore_index=True)
        print("Resulting DataFrame with extracted table data:")
        print(result_df)
        print(f"Shape of resulting DataFrame: {result_df.shape}")

        return result_df
    else:
        print("No tables were extracted from any PDFs.")
        return pd.DataFrame()  # Return an empty DataFrame if no tables were found

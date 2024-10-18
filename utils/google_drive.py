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
from constanst import CREDENTIALS_FILE, SHEET_NAME, WORKSHEET_NAME, PDF_COLUMN

def get_google_sheet_data(sheet_name, worksheet_name, filter_column=None, filter_value=None):
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

    df = pd.DataFrame(filtered_rows, columns=data[0])
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
    try:
        with pdfplumber.open(pdf_file) as pdf:
            for page in pdf.pages:
                tables = page.extract_tables()
                for table in tables:
                    df = pd.DataFrame(table[1:], columns=table[0])
                    tables_data.append(df)
        return tables_data
    except Exception as e:
        print(f"Error al procesar el PDF: {str(e)}")
        return None

def download_pdf_from_drive(file_id, credentials):
    """Downloads a file from Google Drive

    Args:
        file_id (str): ID of the file
        credentials (google.oauth2.credentials.Credentials): Service account credentials

    Returns:
        io.BytesIO: Downloaded file
    """
    if not credentials:
        raise ValueError("No credentials provided")  
    credentials = Credentials.from_service_account_file(CREDENTIALS_FILE, scopes=['https://www.googleapis.com/auth/drive.readonly'])
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

if __name__ == "__main__":
    df = get_google_sheet_data(SHEET_NAME, WORKSHEET_NAME, CREDENTIALS_FILE)

    print("Data loaded from Google Sheets into the DataFrame:")
    print(df)

    pdf_url = df[PDF_COLUMN].iloc[0]

    print("URL of the first PDF:", pdf_url)

    try:
        file_id = extract_file_id_from_url(pdf_url)
        
        pdf_file = download_pdf_from_drive(file_id, CREDENTIALS_FILE)
        print(f"PDF downloaded. Size: {pdf_file.getbuffer().nbytes} bytes")
        
        tables = extract_table_with_pdfplumber(pdf_file)
        
        if tables:
            for i, table in enumerate(tables):
                print(f"Table {i + 1}:")
                print(table)
                print(type(table))
        else:
            print("No tables were found in the PDF or there was an error processing it.")
    except Exception as e:
        print(f"Error processing the PDF: {str(e)}")
import os
from dotenv import load_dotenv

DIR_PATH: str = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
dotenv_path: str = os.path.join(DIR_PATH, '.env')
load_dotenv(dotenv_path)

DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_NAME = os.getenv('DB_NAME')
SHEET_NAME = os.getenv("SHEET_NAME")
WORKSHEET_NAME = os.getenv("WORKSHEET_NAME")
PDF_COLUMN = 'pdf_url'
NOTE_COLUMN = 'note_number'
CREDENTIALS_FILE = os.getenv("CREDENTIALS_FILE")
TEMP_DIR = '/tmp/airflow_data'
DEFAULT_DATES = os.getenv("DEFAULT_DATES")

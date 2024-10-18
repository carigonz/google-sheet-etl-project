import os
from dotenv import load_dotenv

DIR_PATH: str = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
dotenv_path: str = os.path.join(DIR_PATH, '.env')
load_dotenv(dotenv_path)

DBNAME_REDSHIFT = os.getenv('DBNAME')
PASSWORD_REDSHIFT = os.getenv('PASSWORD')
HOST_REDSHIFT = os.getenv('HOST')
PORT_REDSHIFT = os.getenv('PORT')
USER_REDSHIFT = os.getenv('USER')
SHEET_NAME = os.getenv("SHEET_NAME")
WORKSHEET_NAME = os.getenv("WORKSHEET_NAME")
PDF_COLUMN = os.getenv("PDF_COLUMN")
CREDENTIALS_FILE = os.getenv("CREDENTIALS_FILE")
HISTORIC_DOLARS_URL = os.getenv("HISTORIC_DOLARS_URL")
API_URL = os.getenv("API_URL")
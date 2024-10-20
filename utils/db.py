import socket
import urllib.parse
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

from utils.constants import DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME

dbname: str = DB_NAME
user: str = DB_USER
password: str = urllib.parse.quote_plus(str(DB_PASSWORD))
host: str = DB_HOST
port: str = DB_PORT


def create_postgres_connection() -> Engine | Exception:
    """
    Creates and returns a SQLAlchemy engine for connecting to PostgreSQL.

    Returns:
        Engine: A SQLAlchemy Engine object connected to the PostgreSQL database.
    """

    # Check if any of the required environment variables are missing
    if not all([DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME]):
        raise ValueError("Missing required database connection information in environment variables.")
    try:
        connection_string = \
            f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}"
        print(f"Connection string: {connection_string}")

        # Test network connectivity
        try:
            socket.create_connection((DB_HOST, DB_PORT), timeout=5)
            print(f"Network connection to {DB_HOST}:{DB_PORT} successful")
        except socket.error as e:
            print(f"Network connection failed: {e}")

        engine = create_engine(connection_string)
        return engine
    except Exception as e:
        print(f"Error creating PostgreSQL connection: {e}")
        raise e

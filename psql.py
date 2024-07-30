import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

PSQL_HOST = os.environ.get("PSQL_HOST")
PSQL_DB = os.environ.get("PSQL_DB")
PSQL_USER = os.environ.get("PSQL_USER")
PSQL_PASSWORD = os.environ.get("PSQL_PASSWORD")
PSQL_PORT = os.environ.get("PSQL_PORT")
    
def get_db_full_name():
    return f"{PSQL_DB}@{PSQL_HOST}:{PSQL_PORT}"

class PostgreSQL:
    def __init__(self) -> None:
        connection = psycopg2.connect(
            host=PSQL_HOST,
            dbname=PSQL_DB,
            user=PSQL_USER,
            password=PSQL_PASSWORD,
            port=PSQL_PORT
        )
        cursor = connection.cursor()
        
        self.connection = connection
        self.cursor = cursor
        pass

    def db_connection_check(self):
        if self.connection is None:
            return False
        
        try:
            self.connection.cursor()
        except:
            return False
        
        return True
    
    def drop_table_data(self, table):
        query = f"""TRUNCATE {table};
                    DELETE FROM {table};"""
        try:
            self.cursor.execute(query)
            self.connection.commit()
        except Exception as e:
            self.connection.rollback()
            print(f"SQL query {query} was failed: {e}")
            return None

    def write_data(self, query, data):
        try:
            self.cursor.execute(query, data)
            self.connection.commit()
        except Exception as e:
            self.connection.rollback()
            print(f"SQL query {query} with data {data} was failed: {e}")
            return None

    def read_data(self, query):
        try:
            self.cursor.execute(query)
            rows = self.cursor.fetchall()
            return rows
        except Exception as e:
            print(f"SQL query {query} was failed: {e}")
            return None
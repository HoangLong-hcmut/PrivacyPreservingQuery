import pymysql
import os
from dotenv import load_dotenv

load_dotenv()

# Default configuration for local testing
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_USER = os.getenv("DB_USER", "root")
DB_PASSWORD = os.getenv("DB_PASSWORD", "password")
DB_NAME = os.getenv("DB_NAME", "hospital_db")
DB_PORT = int(os.getenv("DB_PORT", 3306))

def get_connection():
    return pymysql.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME,
        port=DB_PORT,
        cursorclass=pymysql.cursors.DictCursor
    )

def execute_query(sql: str, params=None):
    """
    Executes a raw SQL query against the database.
    """
    conn = get_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute(sql, params)
            result = cursor.fetchall()
        conn.commit()
        return result
    finally:
        conn.close()

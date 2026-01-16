import pymysql
import os
from dotenv import load_dotenv

load_dotenv()

# Database Configuration
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_USER = os.getenv("DB_USER", "root")
DB_PASSWORD = os.getenv("DB_PASSWORD", "password")
DB_NAME = os.getenv("DB_NAME", "hospital_db")
DB_PORT = int(os.getenv("DB_PORT", 3306))

_PERSISTENT_CONN = None

def get_connection(force_new=False):
    global _PERSISTENT_CONN
    # Return persistent connection if available and open
    if _PERSISTENT_CONN and not force_new:
        try:
            _PERSISTENT_CONN.ping(reconnect=True)
            return _PERSISTENT_CONN
        except:
             _PERSISTENT_CONN = None

    return pymysql.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME,
        port=DB_PORT,
        cursorclass=pymysql.cursors.DictCursor
    )

class UsePersistentConnection:
    """
    Context manager to reuse a single database connection across multiple execute_query calls.
    Drastically improves performance by avoiding TCP handshake overhead.
    """
    def __enter__(self):
        global _PERSISTENT_CONN
        if not _PERSISTENT_CONN:
            _PERSISTENT_CONN = get_connection()
        return _PERSISTENT_CONN

    def __exit__(self, exc_type, exc_val, exc_tb):
        global _PERSISTENT_CONN
        if _PERSISTENT_CONN:
            try:
                _PERSISTENT_CONN.close()
            except:
                pass
            _PERSISTENT_CONN = None

def execute_query(sql: str, params=None, force_new=False):
    """
    Executes a SQL query and returns the results.
    """
    global _PERSISTENT_CONN
    
    # Check if we are in persistent mode (and NOT forcing new)
    is_persistent = (_PERSISTENT_CONN is not None) and (not force_new)
    
    if is_persistent:
        conn = _PERSISTENT_CONN
    else:
        conn = get_connection(force_new=force_new)

    try:
        with conn.cursor() as cursor:
            cursor.execute(sql, params)
            result = cursor.fetchall()
        conn.commit()
        return result
    finally:
        # Only close if NOT in persistent mode
        if not is_persistent:
            conn.close()

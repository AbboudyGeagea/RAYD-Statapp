cimport os
import contextlib
from typing import List, Dict, Any, Tuple

# We are using psycopg2 for real PostgreSQL interaction
try:
    import psycopg2
    from psycopg2.extras import RealDictCursor
    from psycopg2 import sql
except ImportError:
    print("Warning: psycopg2 is not installed. Please run 'pip install psycopg2-binary' for real database functionality.")
    # Define placeholder classes/functions if psycopg2 is missing to allow the script to be loaded
    class MockPsycopg2:
        class Error(Exception): pass
        def connect(*args, **kwargs): raise ImportError("psycopg2 not found")
    psycopg2 = MockPsycopg2()
    RealDictCursor = object
    sql = object


class DatabaseService:
    """
    A service class responsible for securely managing connections and executing
    parameterized queries against the PostgreSQL Reporting Database using psycopg2.
    """

    def __init__(self, db_config: Dict[str, str]):
        """
        Initializes the service with database connection parameters.

        Args:
            db_config (Dict[str, str]): Dictionary containing connection details
                                        (host, database, user, password, port).
        """
        self.config = db_config
        print(f"DatabaseService initialized. Target DB: {db_config.get('database', 'N/A')}")


    @contextlib.contextmanager
    def get_db_cursor(self, commit_on_exit: bool = False):
        """
        Context manager to establish a database connection and yield a cursor.
        It ensures the connection is closed upon exit and handles transactions.

        Args:
            commit_on_exit (bool): If True, commits the transaction before closing.
        """
        conn = None
        try:
            # Establish the connection using the configuration
            conn = psycopg2.connect(**self.config)
            
            # Yield a cursor that returns results as dictionaries
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                yield cursor
            
            # Handle transaction commit if requested and no exceptions occurred
            if commit_on_exit:
                conn.commit()

        except psycopg2.Error as e:
            # Rollback in case of an error
            if conn:
                conn.rollback()
            print(f"Database Error: {e}")
            raise # Re-raise the exception to be handled by the caller

        except ImportError as e:
            # Handle case where psycopg2 wasn't installed
            print(f"Configuration Error: Cannot connect because {e}")
            raise

        finally:
            # Always close the connection
            if conn:
                conn.close()


    def fetch_all(self, query: str, params: Tuple[Any, ...] = ()) -> List[Dict[str, Any]]:
        """
        Executes a SELECT query and returns all results as a list of dictionaries.

        Args:
            query (str): The SQL SELECT query string.
            params (Tuple): Parameters to be safely injected into the query.

        Returns:
            List[Dict[str, Any]]: The query results.
        """
        print(f"\n--- Executing SELECT Query: {query[:50]}... \nWith Params: {params}")
        
        results = []
        try:
            with self.get_db_cursor(commit_on_exit=False) as cursor:
                # Use execute with parameters for security (prevents SQL injection)
                cursor.execute(query, params)
                results = cursor.fetchall()
        except Exception:
            # The get_db_cursor handles the logging, we just return empty data
            # or allow the exception to propagate if necessary.
            pass

        return results


    def execute_non_query(self, query: str, params: Tuple[Any, ...] = ()) -> bool:
        """
        Executes a non-query command (INSERT, UPDATE, DELETE).

        Args:
            query (str): The SQL non-query string.
            params (Tuple): Parameters to be safely injected into the query.

        Returns:
            bool: True if execution was successful and committed.
        """
        print(f"\n--- Executing NON-QUERY: {query[:50]}... \nWith Params: {params}")

        try:
            # Use the context manager and request a commit on successful exit
            with self.get_db_cursor(commit_on_exit=True) as cursor:
                cursor.execute(query, params)
                # print(f"Rows affected: {cursor.rowcount}")
            print("NON-QUERY successful and committed.")
            return True

        except Exception:
            # Exception handling is done in get_db_cursor, which also rolls back.
            print("NON-QUERY execution failed and rolled back.")
            return False


# =========================================================================
# EXAMPLE USAGE (Will require a running PostgreSQL instance and psycopg2)
# =========================================================================
if __name__ == '__main__':
    # --- IMPORTANT: These values MUST be configured in your Docker/environment ---
    DB_CONFIG = {
        'host': os.environ.get('DB_HOST', 'localhost'),
        'database': os.environ.get('DB_NAME', 'reporting_db'),
        'user': os.environ.get('DB_USER', 'etl_reader'),
        'password': os.environ.get('DB_PASS', 'secure_password'),
        'port': os.environ.get('DB_PORT', '5432')
    }

    # NOTE: This part will fail with an ImportError or connection error 
    # unless psycopg2 is installed and a DB is running at localhost:5432.
    print("\n--- Testing Database Service (Requires live DB connection) ---")
    try:
        service = DatabaseService(DB_CONFIG)

        # Example 1: Fetching data
        access_query = "SELECT report_id, user_group FROM report_access_control LIMIT 1;"
        access_data = service.fetch_all(access_query)
        print("\n[Result 1: Access Control Data]")
        print(access_data)

        # Example 2: Non-query (simulated INSERT for audit log)
        log_query = "INSERT INTO audit_log (user_id, action, timestamp) VALUES (%s, %s, NOW());"
        success = service.execute_non_query(log_query, ('user_999', 'SERVICE_START'))
        print(f"\n[Result 2: Non-Query Success] {success}")

    except Exception as e:
        print(f"\nTEST FAILED: Could not complete DB service test. Ensure PostgreSQL is running and environment variables are set. Error: {e}")

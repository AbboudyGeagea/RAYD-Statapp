# ETL_JOBS/oracle_service.py
import oracledb
from flask import current_app
from db import get_source_db_params   # or from config_service import get_source_db_params

def get_oracle_connection(source_name='oracle_ris'):
    """
    Read credentials from Postgres (SourceDBParams) and return an oracledb connection.
    """
    params = get_source_db_params(source_name)
    dsn = params['dsn']   # "host:port/SID" or full connect string
    user = params['username']
    pwd  = params['password']

    # Use Easy Connect string if that is what you saved: host:port/SID
    conn = oracledb.connect(user=user, password=pwd, dsn=dsn)
    return conn

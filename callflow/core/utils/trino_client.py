# app/db_clients/trino_client.py
import logging
import pandas as pd
from trino.dbapi import connect
from trino.auth import BasicAuthentication
from django.conf import settings
from functools import lru_cache

logger = logging.getLogger(__name__)

print('logger')
print(logger)
# --------------------------------------------------
# Connection (cached)
# --------------------------------------------------
@lru_cache(maxsize=4)
def get_trino_connection():
    """
    Create a cached Trino DB-API connection using HTTPS + Basic auth.
    """
    host = "10.230.180.66"
    port = 8443
    user = "e2e_user"
    password = "6Ex@9XljZn#4"
    catalog = "Clickhouse"
    schema = "default"
    verify = False

    return connect(
        host=host,
        port=port,
        user=user,
        http_scheme="https",
        auth=BasicAuthentication(user, password),
        catalog=catalog,
        schema=schema,
        verify=verify,
    )

# --------------------------------------------------
# Query helper
# --------------------------------------------------
def run_trino_query(sql: str) -> pd.DataFrame:
    """
    Execute Trino query and return all results as DataFrame with detailed logging.
    Automatically retries once with a fresh connection on failure.
    """
    for attempt in range(2):
        try:
            conn = get_trino_connection()
            cursor = conn.cursor()
            
            logger.info("[TRINO] Executing query (attempt %d)...", attempt + 1)
            logger.debug("[TRINO] Query preview: %s", sql)
            print('Sql--- Trino---Test')
            print(sql)
            cursor.execute(sql)
            
            # Get column names
            columns = [col[0] for col in cursor.description]
            logger.info("[TRINO] Columns: %s", columns)
            
            # Fetch ALL rows
            logger.info("[TRINO] Starting fetchall()...")
            rows = cursor.fetchall()
            logger.info("[TRINO] fetchall() complete: %s rows retrieved", f"{len(rows):,}")
            
            # Create DataFrame
            df = pd.DataFrame(rows, columns=columns)
            logger.info("[TRINO] DataFrame created: %s rows, %d columns", f"{len(df):,}", len(df.columns))
            logger.info("[TRINO] DataFrame memory usage: %.2f MB", df.memory_usage(deep=True).sum() / 1024**2)
            
            cursor.close()
            
            return df
        except Exception as e:
            logger.error("[TRINO] Query failed (attempt %d): %s", attempt + 1, e, exc_info=True)
            # Clear cached connection so next attempt gets a fresh one
            get_trino_connection.cache_clear()
            if attempt == 1:
                raise
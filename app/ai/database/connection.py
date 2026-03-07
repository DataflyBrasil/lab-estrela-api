import os
from sqlalchemy import create_engine
import logging
from app.ai.config.settings import settings

logger = logging.getLogger(__name__)

# Fix for pymssql connection issues
os.environ['TDSVER'] = '7.0'

# Create the engine globally to maintain a connection pool
try:
    # Construct connection string for SQL Server with pymssql
    # format: mssql+pymssql://user:password@host:port/database
    DATABASE_URL = f"mssql+pymssql://{settings.DB_USER}:{settings.DB_PASS}@{settings.DB_HOST}:{settings.DB_PORT}/{settings.DB_NAME}"
    
    engine = create_engine(DATABASE_URL, pool_pre_ping=True)
    logger.info("SQLAlchemy engine created successfully.")
except Exception as e:
    logger.error(f"Failed to create SQLAlchemy engine: {e}")
    engine = None

def get_ai_db_engine():
    """
    Returns the SQLAlchemy engine for AI module usage.
    """
    if engine is None:
        raise Exception("Database engine is not initialized.")
    return engine

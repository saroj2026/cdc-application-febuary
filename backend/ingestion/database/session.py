"""Database session management with connection pooling."""

import os
import logging
from typing import Generator, Optional
from sqlalchemy import create_engine, event, pool, text
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.exc import OperationalError, DisconnectionError
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://cdc_user:cdc_pass@72.61.233.209:5432/cdctest")
DATABASE_POOL_SIZE = int(os.getenv("DATABASE_POOL_SIZE", "10"))
DATABASE_MAX_OVERFLOW = int(os.getenv("DATABASE_MAX_OVERFLOW", "20"))

# Create engine with connection error handling
# Use NullPool to avoid connection blocking on startup
# This allows the server to start even if database is unavailable
engine = create_engine(
    DATABASE_URL,
    poolclass=pool.NullPool,  # NullPool doesn't create connections on startup
    pool_pre_ping=True,
    pool_recycle=3600,
    echo=False,
    connect_args={
        "connect_timeout": 30,  # 30 second timeout for initial connection (increased from 10)
        "keepalives": 1,
        "keepalives_idle": 30,
        "keepalives_interval": 10,
        "keepalives_count": 5,
    }
)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Track database availability
_db_available = None

def check_db_connection() -> bool:
    """Check if database is available.
    
    Returns:
        True if database is available, False otherwise
    """
    global _db_available
    try:
        with engine.connect() as conn:
            # Set statement timeout to 25 seconds (less than connection timeout)
            try:
                conn.execute(text("SET statement_timeout = '25s'"))
            except:
                pass  # Some databases may not support this
            conn.execute(text("SELECT 1"))
        _db_available = True
        return True
    except (OperationalError, DisconnectionError) as e:
        logger.warning(f"Database connection check failed: {e}")
        _db_available = False
        return False
    except Exception as e:
        logger.warning(f"Database connection check error: {e}")
        _db_available = False
        return False

def get_db() -> Generator[Optional[Session], None, None]:
    """Get database session with error handling.
    
    Yields:
        Database session or None if connection fails
    """
    global _db_available
    
    # Check if we know the DB is unavailable
    if _db_available is False:
        # Try once more to see if it's back
        if not check_db_connection():
            yield None
            return
    
    db = None
    try:
        db = SessionLocal()
        # Test the connection with statement timeout
        try:
            # Set statement timeout to prevent long-running queries (25 seconds)
            db.execute(text("SET statement_timeout = '25s'"))
        except:
            pass  # Some databases may not support this setting
        db.execute(text("SELECT 1"))
        _db_available = True
    except (OperationalError, DisconnectionError) as e:
        logger.error(f"Database connection error: {e}")
        _db_available = False
        # Clean up failed session
        if db:
            try:
                db.rollback()
            except:
                pass
            try:
                db.close()
            except:
                pass
            db = None
        yield None
        return
    except Exception as e:
        logger.error(f"Unexpected database error: {e}")
        # Clean up failed session
        if db:
            try:
                db.rollback()
            except:
                pass
            try:
                db.close()
            except:
                pass
            db = None
        yield None
        return
    
    # If we get here, db is valid - yield it
    # If an exception is thrown into the generator from outside (e.g., by FastAPI),
    # we need to handle it and ensure cleanup
    try:
        yield db
    except GeneratorExit:
        # Generator is being closed normally or by garbage collector
        # Close the session and re-raise
        if db:
            try:
                db.close()
            except:
                pass
        raise
    except Exception:
        # Exception was thrown into the generator from outside (e.g., by FastAPI)
        # Rollback and re-raise so FastAPI can handle it
        if db:
            try:
                db.rollback()
            except:
                pass
        raise
    finally:
        # Always close the session in finally block
        # This runs whether we yielded normally or an exception was thrown in
        if db:
            try:
                db.close()
            except:
                pass


@event.listens_for(engine, "connect")
def set_sqlite_pragma(dbapi_conn, connection_record):
    """Set database-specific settings on connection."""
    pass

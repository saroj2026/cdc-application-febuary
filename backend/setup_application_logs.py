"""Setup script to create application_logs table and generate initial logs."""

import sys
import os

# Add backend directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from ingestion.database.session import SessionLocal, engine
from ingestion.database.base import Base
from ingestion.database.models_db import ApplicationLogModel
from sqlalchemy import inspect
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def create_table_if_not_exists():
    """Create application_logs table if it doesn't exist."""
    try:
        # Check if table exists
        inspector = inspect(engine)
        tables = inspector.get_table_names()
        
        if 'application_logs' in tables:
            logger.info("✓ Application logs table already exists")
            return True
        
        # Create table
        logger.info("Creating application_logs table...")
        Base.metadata.create_all(bind=engine, tables=[ApplicationLogModel.__table__])
        logger.info("✓ Application logs table created successfully")
        return True
    except Exception as e:
        logger.error(f"Failed to create application_logs table: {e}")
        return False


def generate_initial_logs():
    """Generate some initial sample logs."""
    try:
        db = SessionLocal()
        try:
            from datetime import datetime, timedelta
            import random
            
            # Check if we already have logs
            existing_count = db.query(ApplicationLogModel).count()
            if existing_count > 0:
                logger.info(f"✓ Found {existing_count} existing logs in database")
                return
            
            logger.info("Generating initial sample logs...")
            
            levels = ["INFO", "WARNING", "ERROR"]
            loggers = ["ingestion.api", "ingestion.cdc_manager", "ingestion.pipeline_service"]
            messages = [
                "Application started successfully",
                "Database connection established",
                "CDC Manager initialized",
                "Pipeline service ready",
                "Connection service initialized",
                "Monitoring service started",
                "Health check endpoint active",
                "API server running",
                "Logging system initialized",
                "Application logs table created"
            ]
            
            from datetime import timezone
            base_time = datetime.now(timezone.utc)
            
            for i, message in enumerate(messages):
                level = random.choice(levels)
                logger_name = random.choice(loggers)
                
                log_entry = ApplicationLogModel(
                    level=level,
                    logger=logger_name,
                    message=message,
                    module="api",
                    function="setup_application_logs",
                    line=50 + i,
                    timestamp=base_time - timedelta(minutes=len(messages) - i),
                    extra={"setup": True, "initial": True}
                )
                
                db.add(log_entry)
            
            db.commit()
            logger.info(f"✓ Generated {len(messages)} initial sample logs")
        except Exception as e:
            db.rollback()
            logger.error(f"Failed to generate initial logs: {e}")
        finally:
            db.close()
    except Exception as e:
        logger.error(f"Failed to generate initial logs: {e}")


def main():
    """Main setup function."""
    logger.info("Setting up application logs...")
    
    # Create table
    if not create_table_if_not_exists():
        logger.error("Failed to create table. Please run migration manually:")
        logger.error("  cd backend && python run_migration.py")
        sys.exit(1)
    
    # Generate initial logs
    generate_initial_logs()
    
    logger.info("✓ Application logs setup complete!")
    logger.info("You can now view logs in the frontend Application Logs component.")


if __name__ == "__main__":
    main()


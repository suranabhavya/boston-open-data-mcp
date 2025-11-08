"""
Database Connection Management

This module handles PostgreSQL connections using SQLAlchemy with connection pooling.
It creates a session factory and provides helper functions for database operations.

Key Features:
- Connection pooling for performance
- Automatic session management with context managers
- Health check functionality
- Schema creation and management
"""

import logging
from contextlib import contextmanager
from typing import Generator

from sqlalchemy import create_engine, text, inspect
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.pool import QueuePool
from sqlalchemy.exc import SQLAlchemyError

from config.settings import settings

# Configure logging
logger = logging.getLogger(__name__)


# ============================================================================
# Database Engine Configuration
# ============================================================================

def create_db_engine():
    """
    Create and configure SQLAlchemy engine with connection pooling.
    
    Connection Pool Settings:
    - pool_size=5: Keep 5 connections in the pool
    - max_overflow=10: Allow up to 10 additional connections
    - pool_timeout=30: Wait 30s for a connection before failing
    - pool_recycle=3600: Recycle connections after 1 hour (NeonDB requirement)
    - pool_pre_ping=True: Test connections before using (detect stale connections)
    
    Returns:
        SQLAlchemy Engine instance
    """
    try:
        engine = create_engine(
            settings.database_url_str,
            poolclass=QueuePool,
            pool_size=5,
            max_overflow=10,
            pool_timeout=30,
            pool_recycle=3600,  # Important for NeonDB - recycle connections hourly
            pool_pre_ping=True,  # Test connections before use
            echo=settings.is_development,  # Log SQL in development
        )
        
        logger.info("Database engine created successfully")
        return engine
        
    except Exception as e:
        logger.error(f"Failed to create database engine: {e}")
        raise


# Global engine instance
engine = create_db_engine()


# ============================================================================
# Session Factory
# ============================================================================

# Create a SessionLocal class for creating database sessions
SessionLocal = sessionmaker(
    autocommit=False,  # Don't auto-commit (explicit control)
    autoflush=False,   # Don't auto-flush (explicit control)
    bind=engine,       # Bind to our engine
)


# ============================================================================
# Session Management
# ============================================================================

@contextmanager
def get_db_session() -> Generator[Session, None, None]:
    """
    Context manager for database sessions.
    
    Usage:
        with get_db_session() as session:
            results = session.execute(text("SELECT * FROM table"))
            session.commit()
    
    Features:
    - Automatic rollback on errors
    - Automatic session cleanup
    - Thread-safe
    
    Yields:
        SQLAlchemy Session instance
    """
    session = SessionLocal()
    try:
        yield session
        session.commit()  # Commit if no errors
    except Exception as e:
        session.rollback()  # Rollback on error
        logger.error(f"Database session error: {e}")
        raise
    finally:
        session.close()  # Always close session


def get_db() -> Generator[Session, None, None]:
    """
    Dependency injection for FastAPI endpoints.
    
    Usage in FastAPI:
        @app.get("/items")
        def get_items(db: Session = Depends(get_db)):
            return db.query(Item).all()
    
    Yields:
        SQLAlchemy Session instance
    """
    session = SessionLocal()
    try:
        yield session
    finally:
        session.close()


# ============================================================================
# Database Health & Utilities
# ============================================================================

def check_database_health() -> bool:
    """
    Check if database connection is working.
    
    Returns:
        True if database is accessible, False otherwise
    """
    try:
        with get_db_session() as session:
            session.execute(text("SELECT 1"))
        logger.info("Database health check passed")
        return True
    except Exception as e:
        logger.error(f"Database health check failed: {e}")
        return False


def ensure_schema_exists() -> None:
    """
    Ensure the boston_data schema exists in the database.
    
    Creates the schema if it doesn't exist.
    This should be called on application startup.
    """
    try:
        with get_db_session() as session:
            # Check if schema exists
            result = session.execute(
                text(
                    "SELECT schema_name FROM information_schema.schemata "
                    "WHERE schema_name = :schema_name"
                ),
                {"schema_name": settings.database_schema}
            )
            
            if not result.fetchone():
                # Create schema
                session.execute(
                    text(f"CREATE SCHEMA IF NOT EXISTS {settings.database_schema}")
                )
                session.commit()
                logger.info(f"Created schema: {settings.database_schema}")
            else:
                logger.info(f"Schema exists: {settings.database_schema}")
                
    except Exception as e:
        logger.error(f"Failed to ensure schema exists: {e}")
        raise


def ensure_postgis_extension() -> None:
    """
    Ensure PostGIS extension is enabled.
    
    PostGIS provides geographic/spatial operations for distance calculations.
    This should already be enabled from the NeonDB setup, but we verify here.
    """
    try:
        with get_db_session() as session:
            # Check if PostGIS is installed
            result = session.execute(
                text(
                    "SELECT extname FROM pg_extension "
                    "WHERE extname = 'postgis'"
                )
            )
            
            if not result.fetchone():
                # Try to enable PostGIS (may require superuser)
                try:
                    session.execute(text("CREATE EXTENSION IF NOT EXISTS postgis"))
                    session.commit()
                    logger.info("PostGIS extension enabled")
                except SQLAlchemyError:
                    logger.warning(
                        "Could not enable PostGIS. "
                        "Please enable it manually in NeonDB SQL Editor: "
                        "CREATE EXTENSION IF NOT EXISTS postgis;"
                    )
            else:
                logger.info("PostGIS extension is enabled")
                
    except Exception as e:
        logger.error(f"Failed to check PostGIS extension: {e}")
        # Don't raise - PostGIS is optional for basic functionality


def get_table_names(schema: str = None) -> list[str]:
    """
    Get list of all tables in the database.
    
    Args:
        schema: Schema name (default: settings.database_schema)
        
    Returns:
        List of table names
    """
    schema = schema or settings.database_schema
    
    try:
        inspector = inspect(engine)
        tables = inspector.get_table_names(schema=schema)
        logger.info(f"Found {len(tables)} tables in schema '{schema}'")
        return tables
    except Exception as e:
        logger.error(f"Failed to get table names: {e}")
        return []


def drop_all_tables(schema: str = None) -> None:
    """
    Drop all tables in the specified schema.
    
    WARNING: This is destructive! Use only for development/testing.
    
    Args:
        schema: Schema name (default: settings.database_schema)
    """
    schema = schema or settings.database_schema
    
    if settings.is_production:
        raise RuntimeError("Cannot drop tables in production!")
    
    try:
        with get_db_session() as session:
            # Get all tables
            tables = get_table_names(schema)
            
            # Drop each table
            for table in tables:
                session.execute(
                    text(f'DROP TABLE IF EXISTS "{schema}"."{table}" CASCADE')
                )
            
            session.commit()
            logger.warning(f"Dropped {len(tables)} tables from schema '{schema}'")
            
    except Exception as e:
        logger.error(f"Failed to drop tables: {e}")
        raise


# ============================================================================
# Initialization
# ============================================================================

def initialize_database() -> None:
    """
    Initialize database on application startup.
    
    This function:
    1. Checks database health
    2. Ensures schema exists
    3. Verifies PostGIS extension
    
    Should be called when the FastAPI app starts.
    """
    logger.info("🔄 Initializing database...")
    
    # Check health
    if not check_database_health():
        raise RuntimeError("Database is not accessible!")
    
    # Ensure schema exists
    ensure_schema_exists()
    
    # Check PostGIS
    ensure_postgis_extension()
    
    logger.info("Database initialization complete")


# ============================================================================
# Startup Check
# ============================================================================

if __name__ == "__main__":
    # Test database connection when run directly
    logging.basicConfig(level=logging.INFO)
    initialize_database()
    
    # Print database info
    tables = get_table_names()
    print(f"\nDatabase Status:")
    print(f"   Schema: {settings.database_schema}")
    print(f"   Tables: {len(tables)}")
    if tables:
        print(f"   Table names: {', '.join(tables)}")


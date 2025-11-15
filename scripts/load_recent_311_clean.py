#!/usr/bin/env python3
"""
Load Recent 311 Service Requests with Date Validation

This script loads recent 311 data and filters out any bad dates.
Uses simple API calls without complex SQL filtering.
"""

import logging
import sys
import pandas as pd
from pathlib import Path
from datetime import datetime

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from db.connection import get_db_session
from db.models import ServiceRequest
from datasets.service_requests import ServiceRequestsConnector

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def validate_dates(df: pd.DataFrame) -> pd.DataFrame:
    """Remove records with invalid dates"""
    initial_count = len(df)

    # Convert dates
    for col in ['open_dt', 'closed_dt', 'target_dt']:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')

    # Filter out bad dates for open_dt (required field)
    if 'open_dt' in df.columns:
        df = df[
            (df['open_dt'].notna()) &
            (df['open_dt'] >= pd.Timestamp('1900-01-01')) &
            (df['open_dt'] <= pd.Timestamp('2100-01-01'))
        ]

    removed = initial_count - len(df)
    if removed > 0:
        logger.warning(f"Removed {removed} records with invalid dates")

    return df

def delete_all_service_requests():
    """Delete all existing service request records"""
    logger.info("=" * 80)
    logger.info("1. DELETING ALL EXISTING SERVICE REQUESTS")
    logger.info("=" * 80)

    with get_db_session() as session:
        count_before = session.query(ServiceRequest).count()
        logger.info(f"Records before delete: {count_before}")

        session.query(ServiceRequest).delete()
        session.commit()

        count_after = session.query(ServiceRequest).count()
        logger.info(f"Records after delete: {count_after}")
        logger.info(f"✅ Deleted {count_before} records\n")

def load_new_system():
    """Load data from NEW 311 system"""
    logger.info("=" * 80)
    logger.info("2. LOADING NEW SYSTEM DATA")
    logger.info("=" * 80)

    try:
        connector = ServiceRequestsConnector(use_old_system=False)

        # Fetch recent data (API will return most recent by default)
        logger.info("Fetching recent data from NEW system...")
        df = connector.fetch_recent(limit=5000, clean=False)

        if df.empty:
            logger.warning("No data returned from NEW system")
            return 0

        logger.info(f"Fetched {len(df)} records")

        # Validate dates BEFORE cleaning
        df = validate_dates(df)
        logger.info(f"After date validation: {len(df)} records")

        # Clean data
        df = connector.clean_data(df)
        logger.info(f"After cleaning: {len(df)} records")

        # Load to database
        count = connector.load_data(df, upsert=True)
        logger.info(f"✅ Loaded {count} records from NEW system\n")
        return count

    except Exception as e:
        logger.error(f"❌ Error loading NEW system: {e}")
        import traceback
        traceback.print_exc()
        return 0

def load_old_system():
    """Load data from OLD 311 system"""
    logger.info("=" * 80)
    logger.info("3. LOADING OLD SYSTEM DATA")
    logger.info("=" * 80)

    try:
        connector = ServiceRequestsConnector(use_old_system=True)

        # Fetch recent data
        logger.info("Fetching recent data from OLD system...")
        df = connector.fetch_recent(limit=5000, clean=False)

        if df.empty:
            logger.warning("No data returned from OLD system")
            return 0

        logger.info(f"Fetched {len(df)} records")

        # Validate dates BEFORE cleaning
        df = validate_dates(df)
        logger.info(f"After date validation: {len(df)} records")

        # Clean data
        df = connector.clean_data(df)
        logger.info(f"After cleaning: {len(df)} records")

        # Remove duplicates (check against NEW system records)
        with get_db_session() as session:
            if 'case_enquiry_id' in df.columns:
                existing_ids = set()
                for case_id in df['case_enquiry_id'].unique():
                    if pd.notna(case_id):
                        exists = session.query(ServiceRequest).filter_by(case_enquiry_id=case_id).first()
                        if exists:
                            existing_ids.add(case_id)

                if existing_ids:
                    df = df[~df['case_enquiry_id'].isin(existing_ids)]
                    logger.info(f"Removed {len(existing_ids)} duplicates")
                    logger.info(f"After deduplication: {len(df)} records")

        # Load to database
        count = connector.load_data(df, upsert=True)
        logger.info(f"✅ Loaded {count} records from OLD system\n")
        return count

    except Exception as e:
        logger.error(f"❌ Error loading OLD system: {e}")
        import traceback
        traceback.print_exc()
        return 0

def verify_data():
    """Verify the loaded data"""
    logger.info("=" * 80)
    logger.info("4. VERIFYING DATA")
    logger.info("=" * 80)

    with get_db_session() as session:
        total_count = session.query(ServiceRequest).count()
        logger.info(f"Total records: {total_count}")

        if total_count == 0:
            logger.warning("⚠️  No data in database!")
            return

        # Check date range
        from sqlalchemy import func
        result = session.query(
            func.min(ServiceRequest.open_dt).label('min_date'),
            func.max(ServiceRequest.open_dt).label('max_date')
        ).first()

        logger.info(f"Date range: {result.min_date} to {result.max_date}")

        # Check for bad dates
        bad_old = session.query(ServiceRequest).filter(ServiceRequest.open_dt < datetime(1900, 1, 1)).count()
        bad_future = session.query(ServiceRequest).filter(ServiceRequest.open_dt > datetime(2100, 1, 1)).count()

        logger.info(f"Bad dates (before 1900): {bad_old}")
        logger.info(f"Bad dates (after 2100): {bad_future}")

        # Check by status
        from sqlalchemy import text
        status_counts = session.query(
            ServiceRequest.case_status,
            func.count(ServiceRequest.case_enquiry_id).label('count')
        ).group_by(ServiceRequest.case_status).order_by(text('count DESC')).limit(5).all()

        logger.info(f"\nTop 5 statuses:")
        for status, count in status_counts:
            logger.info(f"  - {status or 'NULL'}: {count}")

        if bad_old > 0 or bad_future > 0:
            logger.warning("\n⚠️  WARNING: Found records with invalid dates!")
        else:
            logger.info("\n✅ All dates are valid!")

def main():
    """Main execution"""
    logger.info("\n" + "=" * 80)
    logger.info("LOAD RECENT 311 SERVICE REQUESTS")
    logger.info("=" * 80 + "\n")

    try:
        # Step 1: Delete all existing data
        delete_all_service_requests()

        # Step 2: Load new system data
        new_count = load_new_system()

        # Step 3: Load old system data
        old_count = load_old_system()

        # Step 4: Verify
        verify_data()

        logger.info("\n" + "=" * 80)
        logger.info("✅ DATA LOAD COMPLETE!")
        logger.info("=" * 80)
        logger.info(f"NEW system: {new_count} records")
        logger.info(f"OLD system: {old_count} records")
        logger.info(f"TOTAL: {new_count + old_count} records")
        logger.info("=" * 80 + "\n")

    except KeyboardInterrupt:
        logger.warning("\n⚠️  Data load interrupted by user\n")
        sys.exit(1)
    except Exception as e:
        logger.error(f"\n❌ ERROR: {e}\n")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()

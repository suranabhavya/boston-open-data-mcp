"""
Verify Data in Neon Database

This script verifies the data loaded into Neon DB and checks for duplicates.
"""

import logging
import sys
from pathlib import Path
from datetime import datetime, timedelta

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from db.connection import get_db_session
from db.models import BuildingViolation, CrimeIncident, ServiceRequest
from sqlalchemy import func, and_

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def get_date_2_years_ago():
    """Get date from 2 years ago."""
    return datetime.now() - timedelta(days=730)


def verify_building_violations():
    """Verify building violations data."""
    logger.info("="*70)
    logger.info("Building Violations Verification")
    logger.info("="*70)

    with get_db_session() as session:
        # Total count
        total = session.query(func.count(BuildingViolation.case_no)).scalar()
        logger.info(f"Total building violations: {total}")

        # Count from last 2 years
        cutoff_date = get_date_2_years_ago()
        last_2_years = session.query(func.count(BuildingViolation.case_no)).filter(
            BuildingViolation.status_dttm >= cutoff_date
        ).scalar()
        logger.info(f"Last 2 years: {last_2_years}")

        # Check for duplicates
        duplicates = session.query(
            BuildingViolation.case_no,
            func.count(BuildingViolation.case_no).label('count')
        ).group_by(
            BuildingViolation.case_no
        ).having(
            func.count(BuildingViolation.case_no) > 1
        ).all()

        if duplicates:
            logger.warning(f"Found {len(duplicates)} duplicate case_no values!")
            for case_no, count in duplicates[:5]:
                logger.warning(f"  {case_no}: {count} occurrences")
        else:
            logger.info("No duplicates found - all case_no values are unique!")

        # Date range
        min_date = session.query(func.min(BuildingViolation.status_dttm)).scalar()
        max_date = session.query(func.max(BuildingViolation.status_dttm)).scalar()
        logger.info(f"Date range: {min_date} to {max_date}")

        return {
            'total': total,
            'last_2_years': last_2_years,
            'duplicates': len(duplicates) if duplicates else 0
        }


def verify_crime_incidents():
    """Verify crime incidents data."""
    logger.info("\n" + "="*70)
    logger.info("Crime Incidents Verification")
    logger.info("="*70)

    with get_db_session() as session:
        # Total count
        total = session.query(func.count(CrimeIncident.incident_number)).scalar()
        logger.info(f"Total crime incidents: {total}")

        # Count from last 2 years
        cutoff_date = get_date_2_years_ago()
        last_2_years = session.query(func.count(CrimeIncident.incident_number)).filter(
            CrimeIncident.occurred_on_date >= cutoff_date
        ).scalar()
        logger.info(f"Last 2 years: {last_2_years}")

        # Check for duplicates
        duplicates = session.query(
            CrimeIncident.incident_number,
            func.count(CrimeIncident.incident_number).label('count')
        ).group_by(
            CrimeIncident.incident_number
        ).having(
            func.count(CrimeIncident.incident_number) > 1
        ).all()

        if duplicates:
            logger.warning(f"Found {len(duplicates)} duplicate incident_number values!")
            for incident_number, count in duplicates[:5]:
                logger.warning(f"  {incident_number}: {count} occurrences")
        else:
            logger.info("No duplicates found - all incident_number values are unique!")

        # Date range
        min_date = session.query(func.min(CrimeIncident.occurred_on_date)).scalar()
        max_date = session.query(func.max(CrimeIncident.occurred_on_date)).scalar()
        logger.info(f"Date range: {min_date} to {max_date}")

        return {
            'total': total,
            'last_2_years': last_2_years,
            'duplicates': len(duplicates) if duplicates else 0
        }


def verify_service_requests():
    """Verify service requests data."""
    logger.info("\n" + "="*70)
    logger.info("Service Requests Verification")
    logger.info("="*70)

    with get_db_session() as session:
        # Total count
        total = session.query(func.count(ServiceRequest.case_enquiry_id)).scalar()
        logger.info(f"Total service requests: {total}")

        # Count from last 2 years
        cutoff_date = get_date_2_years_ago()
        last_2_years = session.query(func.count(ServiceRequest.case_enquiry_id)).filter(
            ServiceRequest.open_dt >= cutoff_date
        ).scalar()
        logger.info(f"Last 2 years: {last_2_years}")

        # Check for duplicates
        duplicates = session.query(
            ServiceRequest.case_enquiry_id,
            func.count(ServiceRequest.case_enquiry_id).label('count')
        ).group_by(
            ServiceRequest.case_enquiry_id
        ).having(
            func.count(ServiceRequest.case_enquiry_id) > 1
        ).all()

        if duplicates:
            logger.warning(f"Found {len(duplicates)} duplicate case_enquiry_id values!")
            for case_id, count in duplicates[:5]:
                logger.warning(f"  {case_id}: {count} occurrences")
        else:
            logger.info("No duplicates found - all case_enquiry_id values are unique!")

        # Date range
        min_date = session.query(func.min(ServiceRequest.open_dt)).scalar()
        max_date = session.query(func.max(ServiceRequest.open_dt)).scalar()
        logger.info(f"Date range: {min_date} to {max_date}")

        return {
            'total': total,
            'last_2_years': last_2_years,
            'duplicates': len(duplicates) if duplicates else 0
        }


def main():
    """Main entry point."""
    logger.info("\n" + "Boston Open Data - Database Verification")
    logger.info(f"Checking data from: {get_date_2_years_ago().strftime('%Y-%m-%d')} onwards\n")

    try:
        # Verify each dataset
        bv_stats = verify_building_violations()
        ci_stats = verify_crime_incidents()
        sr_stats = verify_service_requests()

        # Summary
        logger.info("\n" + "="*70)
        logger.info("SUMMARY")
        logger.info("="*70)
        logger.info(f"Building Violations:")
        logger.info(f"  Total records: {bv_stats['total']}")
        logger.info(f"  Last 2 years: {bv_stats['last_2_years']}")
        logger.info(f"  Duplicates: {bv_stats['duplicates']}")

        logger.info(f"\nCrime Incidents:")
        logger.info(f"  Total records: {ci_stats['total']}")
        logger.info(f"  Last 2 years: {ci_stats['last_2_years']}")
        logger.info(f"  Duplicates: {ci_stats['duplicates']}")

        logger.info(f"\nService Requests:")
        logger.info(f"  Total records: {sr_stats['total']}")
        logger.info(f"  Last 2 years: {sr_stats['last_2_years']}")
        logger.info(f"  Duplicates: {sr_stats['duplicates']}")

        total_records = bv_stats['total'] + ci_stats['total'] + sr_stats['total']
        total_2_years = bv_stats['last_2_years'] + ci_stats['last_2_years'] + sr_stats['last_2_years']
        total_dupes = bv_stats['duplicates'] + ci_stats['duplicates'] + sr_stats['duplicates']

        logger.info(f"\nGRAND TOTAL:")
        logger.info(f"  All records: {total_records}")
        logger.info(f"  Last 2 years: {total_2_years}")
        logger.info(f"  Total duplicates: {total_dupes}")

        if total_dupes == 0:
            logger.info("\n✅ SUCCESS: No duplicates found! Upsert worked correctly.")
        else:
            logger.warning(f"\n⚠️  WARNING: Found {total_dupes} duplicate records!")

        logger.info("="*70 + "\n")

    except Exception as e:
        logger.error(f"\nVerification failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()

"""
Load Last 2 Years Data Script

This script loads the last 2 years of data from Boston Open Data API into the database.

Usage:
    python scripts/load_last_2_years.py
"""

import logging
import sys
from pathlib import Path
from datetime import datetime, timedelta

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from datasets.crime_incidents import crime_connector
from datasets.service_requests import service_requests_connector
from datasets.building_violations import building_violations_connector

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def get_date_2_years_ago():
    """Get date from 2 years ago in YYYY-MM-DD format."""
    two_years_ago = datetime.now() - timedelta(days=730)  # 2 years = ~730 days
    return two_years_ago.strftime('%Y-%m-%d')


def load_building_violations_2_years():
    """Load last 2 years of building violations data."""
    logger.info("="*70)
    logger.info("Loading Building Violations Data (Last 2 Years)")
    logger.info("="*70)

    try:
        cutoff_date = get_date_2_years_ago()
        logger.info(f"Fetching records from {cutoff_date} onwards...")

        # Build SQL query with date filter
        sql = f"""
        SELECT * FROM "{building_violations_connector.resource_id}"
        WHERE "status_dttm" >= '{cutoff_date}'
        ORDER BY "status_dttm" DESC
        """

        logger.info(f"SQL Query: {sql}")

        # Make SQL API request
        params = {"sql": sql}
        response_data = building_violations_connector._make_api_request("datastore_search_sql", params)

        # Extract records
        import pandas as pd
        records = response_data.get("result", {}).get("records", [])

        if not records:
            logger.warning("No data fetched")
            return 0

        logger.info(f"Fetched {len(records)} records")
        df = pd.DataFrame(records)

        # Clean data
        df = building_violations_connector.clean_data(df)
        logger.info(f"Cleaned data: {len(df)} records remaining")

        # Load data
        count = building_violations_connector.load_data(df, upsert=True)
        logger.info(f"Successfully loaded {count} building violation records")
        return count
    except Exception as e:
        logger.error(f"Failed to load building violations data: {e}")
        import traceback
        traceback.print_exc()
        return 0


def load_crime_data_2_years():
    """Load last 2 years of crime incident data."""
    logger.info("="*70)
    logger.info("Loading Crime Incident Data (Last 2 Years)")
    logger.info("="*70)

    try:
        cutoff_date = get_date_2_years_ago()
        logger.info(f"Fetching records from {cutoff_date} onwards...")

        # Build SQL query with date filter
        sql = f"""
        SELECT * FROM "{crime_connector.resource_id}"
        WHERE "OCCURRED_ON_DATE" >= '{cutoff_date}'
        ORDER BY "OCCURRED_ON_DATE" DESC
        """

        logger.info(f"SQL Query: {sql}")

        # Make SQL API request
        params = {"sql": sql}
        response_data = crime_connector._make_api_request("datastore_search_sql", params)

        # Extract records
        import pandas as pd
        records = response_data.get("result", {}).get("records", [])

        if not records:
            logger.warning("No data fetched")
            return 0

        logger.info(f"Fetched {len(records)} records")
        df = pd.DataFrame(records)

        # Clean data
        df = crime_connector.clean_data(df)
        logger.info(f"Cleaned data: {len(df)} records remaining")

        # Load data
        count = crime_connector.load_data(df, upsert=True)
        logger.info(f"Successfully loaded {count} crime records")
        return count
    except Exception as e:
        logger.error(f"Failed to load crime data: {e}")
        import traceback
        traceback.print_exc()
        return 0


def load_service_requests_2_years():
    """Load last 2 years of 311 service request data."""
    logger.info("="*70)
    logger.info("Loading 311 Service Request Data (Last 2 Years)")
    logger.info("="*70)

    try:
        cutoff_date = get_date_2_years_ago()
        logger.info(f"Fetching records from {cutoff_date} onwards...")

        # Build SQL query with date filter
        sql = f"""
        SELECT * FROM "{service_requests_connector.resource_id}"
        WHERE "open_date" >= '{cutoff_date}'
        ORDER BY "open_date" DESC
        """

        logger.info(f"SQL Query: {sql}")

        # Make SQL API request
        params = {"sql": sql}
        response_data = service_requests_connector._make_api_request("datastore_search_sql", params)

        # Extract records
        import pandas as pd
        records = response_data.get("result", {}).get("records", [])

        if not records:
            logger.warning("No data fetched")
            return 0

        logger.info(f"Fetched {len(records)} records")
        df = pd.DataFrame(records)

        # Clean data
        df = service_requests_connector.clean_data(df)
        logger.info(f"Cleaned data: {len(df)} records remaining")

        # Load data
        count = service_requests_connector.load_data(df, upsert=True)
        logger.info(f"Successfully loaded {count} service request records")
        return count
    except Exception as e:
        logger.error(f"Failed to load service request data: {e}")
        import traceback
        traceback.print_exc()
        return 0


def main():
    """Main entry point."""
    logger.info("\n" + "Boston Open Data MCP Server - Load Last 2 Years Data")
    logger.info(f"Date cutoff: {get_date_2_years_ago()}\n")

    results = {}

    try:
        # Load building violations
        logger.info("\n[1/3] Building Violations")
        results['building_violations'] = load_building_violations_2_years()

        # Load crime data
        logger.info("\n[2/3] Crime Incidents")
        results['crime'] = load_crime_data_2_years()

        # Load service requests
        logger.info("\n[3/3] Service Requests")
        results['service_requests'] = load_service_requests_2_years()

        # Summary
        logger.info("\n" + "="*70)
        logger.info("Load Summary")
        logger.info("="*70)
        for dataset, count in results.items():
            logger.info(f"  {dataset}: {count} records")
        logger.info(f"  Total: {sum(results.values())} records")
        logger.info("="*70 + "\n")

        logger.info("\nData load completed successfully!\n")

    except KeyboardInterrupt:
        logger.warning("\nData load interrupted by user\n")
        sys.exit(1)
    except Exception as e:
        logger.error(f"\nData load failed: {e}\n")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()

"""
Load Initial Data Script

This script loads initial data from Boston Open Data API into the database.
Use this for first-time setup or to refresh all data.

Usage:
    python scripts/load_initial_data.py --dataset crime --limit 1000
    python scripts/load_initial_data.py --dataset all --limit 5000
"""

import argparse
import logging
import sys
from pathlib import Path

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


def load_crime_data(limit=None):
    """Load crime incident data sorted by most recent occurrence date."""
    logger.info("="*70)
    logger.info("Loading Crime Incident Data (Most Recent)")
    logger.info("="*70)
    
    try:
        # Fetch with SQL sorting by OCCURRED_ON_DATE DESC
        logger.info("Fetching data sorted by occurrence date (most recent first)...")
        df = crime_connector.fetch_data(
            limit=limit,
            sort_field="OCCURRED_ON_DATE",
            sort_order="DESC"
        )
        
        if df.empty:
            logger.warning("No data fetched")
            return 0
        
        logger.info(f"Fetched {len(df)} records")
        
        # Clean data
        df = crime_connector.clean_data(df)
        logger.info(f"Cleaned data: {len(df)} records remaining")
        
        # Load data
        count = crime_connector.load_data(df, upsert=True)
        logger.info(f"Successfully loaded {count} crime records")
        return count
    except Exception as e:
        logger.error(f"Failed to load crime data: {e}")
        return 0


def load_service_requests(limit=None):
    """Load 311 service request data sorted by most recent open date."""
    logger.info("="*70)
    logger.info("Loading 311 Service Request Data (Most Recent)")
    logger.info("="*70)
    
    try:
        # Fetch with SQL sorting by open_date DESC
        logger.info("Fetching data sorted by open date (most recent first)...")
        df = service_requests_connector.fetch_data(
            limit=limit,
            sort_field="open_date",
            sort_order="DESC"
        )
        
        if df.empty:
            logger.warning("No data fetched")
            return 0
        
        logger.info(f"Fetched {len(df)} records")
        
        # Clean data
        df = service_requests_connector.clean_data(df)
        logger.info(f"Cleaned data: {len(df)} records remaining")
        
        # Load data
        count = service_requests_connector.load_data(df, upsert=True)
        logger.info(f"Successfully loaded {count} service request records")
        return count
    except Exception as e:
        logger.error(f"Failed to load service request data: {e}")
        return 0


def load_building_violations(limit=None):
    """Load building violations data sorted by most recent status date."""
    logger.info("="*70)
    logger.info("Loading Building Violations Data (Most Recent)")
    logger.info("="*70)
    
    try:
        # Fetch with SQL sorting by status_dttm DESC
        logger.info("Fetching data sorted by status date (most recent first)...")
        df = building_violations_connector.fetch_data(
            limit=limit,
            sort_field="status_dttm",
            sort_order="DESC"
        )
        
        if df.empty:
            logger.warning("No data fetched")
            return 0
        
        logger.info(f"Fetched {len(df)} records")
        
        # Clean data
        df = building_violations_connector.clean_data(df)
        logger.info(f"Cleaned data: {len(df)} records remaining")
        
        # Load data
        count = building_violations_connector.load_data(df, upsert=True)
        logger.info(f"Successfully loaded {count} building violation records")
        return count
    except Exception as e:
        logger.error(f"Failed to load building violations data: {e}")
        return 0


def load_all_data(limit=None):
    """Load all datasets."""
    logger.info("\n" + "="*70)
    logger.info("Loading All Datasets")
    logger.info("="*70 + "\n")
    
    results = {}
    
    # Load crime data
    results['crime'] = load_crime_data(limit)
    
    # Load service requests
    results['service_requests'] = load_service_requests(limit)
    
    # Load building violations
    results['building_violations'] = load_building_violations(limit)
    
    # Summary
    logger.info("\n" + "="*70)
    logger.info("Load Summary")
    logger.info("="*70)
    for dataset, count in results.items():
        logger.info(f"  {dataset}: {count} records")
    logger.info(f"  Total: {sum(results.values())} records")
    logger.info("="*70 + "\n")
    
    return results


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description='Load initial data from Boston Open Data API'
    )
    parser.add_argument(
        '--dataset',
        choices=['crime', 'service_requests', 'building_violations', 'all'],
        default='all',
        help='Which dataset to load (default: all)'
    )
    parser.add_argument(
        '--limit',
        type=int,
        default=None,
        help='Maximum number of records to load per dataset (default: no limit)'
    )
    
    args = parser.parse_args()
    
    logger.info("\n" + "Boston Open Data MCP Server - Initial Data Load")
    logger.info(f"Dataset: {args.dataset}")
    logger.info(f"Limit: {args.limit if args.limit else 'No limit'}\n")
    
    try:
        if args.dataset == 'crime':
            load_crime_data(args.limit)
        elif args.dataset == 'service_requests':
            load_service_requests(args.limit)
        elif args.dataset == 'building_violations':
            load_building_violations(args.limit)
        elif args.dataset == 'all':
            load_all_data(args.limit)
        
        logger.info("\nData load completed successfully!\n")
        
    except KeyboardInterrupt:
        logger.warning("\nData load interrupted by user\n")
        sys.exit(1)
    except Exception as e:
        logger.error(f"\nData load failed: {e}\n")
        sys.exit(1)


if __name__ == "__main__":
    main()


"""
311 Service Requests Dataset Connector

Fetches and processes Boston 311 non-emergency service requests.
Updated daily from Boston Open Data Portal.

Dataset: 311 Service Requests
Source: https://data.boston.gov/dataset/311-service-requests
"""

import logging
from typing import Optional
import pandas as pd

from datasets.base import BaseDatasetConnector, validate_boston_coordinates
from db.models import ServiceRequest

logger = logging.getLogger(__name__)


class ServiceRequestsConnector(BaseDatasetConnector):
    """
    Connector for Boston 311 service request data.

    Handles fetching, cleaning, and loading 311 service requests from both:
    - NEW system (2024+): 254adca6-64ab-4c5c-9fc0-a6da622be185
    - OLD system (pre-2024): 9d7c2214-4709-478a-a2e8-fb2020a5bb94
    """

    # Boston Open Data resource IDs
    NEW_RESOURCE_ID = "254adca6-64ab-4c5c-9fc0-a6da622be185"  # New system
    OLD_RESOURCE_ID = "9d7c2214-4709-478a-a2e8-fb2020a5bb94"  # Old system (legacy)

    # Default to new system for backwards compatibility
    RESOURCE_ID = NEW_RESOURCE_ID

    def __init__(self, use_old_system: bool = False):
        """
        Initialize the connector.

        Args:
            use_old_system: If True, use the old system resource ID
        """
        resource_id = self.OLD_RESOURCE_ID if use_old_system else self.NEW_RESOURCE_ID
        super().__init__(
            resource_id=resource_id,
            table_name="service_requests"
        )
        self.use_old_system = use_old_system
    
    def fetch_recent(
        self, 
        limit: int = 500,
        clean: bool = True
    ) -> pd.DataFrame:
        """
        Fetch most recent 311 service requests sorted by open date.
        
        Args:
            limit: Number of recent records to fetch
            clean: Whether to clean the data
            
        Returns:
            DataFrame with recent service requests
        """
        logger.info(f"Fetching {limit} most recent service requests...")
        
        # Use SQL sort to get truly recent requests
        df = self.fetch_data(
            limit=limit,
            sort_field="open_date",
            sort_order="DESC"
        )
        
        if clean and not df.empty:
            df = self.clean_data(df)
        
        return df
    
    def get_model(self):
        """Return the ServiceRequest SQLAlchemy model."""
        return ServiceRequest
    
    def clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean and transform 311 service request data from both old and new systems.

        Args:
            df: Raw pandas DataFrame from API

        Returns:
            Cleaned pandas DataFrame ready for database
        """
        logger.info(f"Cleaning {len(df)} service request records...")
        logger.info(f"System: {'OLD (pre-2024)' if self.use_old_system else 'NEW (2024+)'}")

        df = df.copy()

        # =====================================================================
        # Column Mapping (different for old vs new system)
        # =====================================================================
        if self.use_old_system:
            # OLD system field mapping
            column_mapping = {
                'case_enquiry_id': 'case_enquiry_id',  # Already correct
                'open_dt': 'open_dt',                   # Already correct
                'sla_target_dt': 'target_dt',           # Map to target_dt
                'closed_dt': 'closed_dt',               # Already correct
                'case_title': 'case_title',             # Already correct (also map to type)
                'subject': 'subject',                   # Already correct
                'reason': 'reason',                     # Already correct
                'type': 'type',                         # Already correct
                'department': 'department',             # Already correct
                'location': 'address',                  # Full address
                'location_street_name': 'street_name_old',  # Backup if location missing
                'location_zipcode': 'zipcode',          # Zip code
                'latitude': 'latitude',
                'longitude': 'longitude',
                'ward': 'ward',
                'neighborhood': 'neighborhood',
                'submitted_photo': 'submittedphoto',
                'closed_photo': 'closedphoto',
                'case_status': 'case_status',
                'closure_reason': 'closure_reason_old',  # Keep for merging
            }
        else:
            # NEW system field mapping
            column_mapping = {
                'case_id': 'case_enquiry_id',      # API uses case_id, DB uses case_enquiry_id
                'open_date': 'open_dt',
                'target_close_date': 'target_dt',
                'close_date': 'closed_dt',
                'case_status': 'case_status',
                'case_topic': 'case_title',        # API uses case_topic
                'service_name': 'subject',         # API uses service_name
                'closure_reason': 'reason',        # API uses closure_reason
                'assigned_department': 'department',  # API uses assigned_department
                'submitted_photo': 'submittedphoto',  # API uses submitted_photo
                'closed_photo': 'closedphoto',     # API uses closed_photo
                'latitude': 'latitude',
                'longitude': 'longitude',
                'ward': 'ward',
                'neighborhood': 'neighborhood',
                'full_address': 'address',  # API uses full_address
                'zip_code': 'zipcode',  # API uses zip_code
            }

        # Also map case_topic/case_title to type field for consistency
        if 'case_topic' in df.columns:
            df['type'] = df['case_topic']
        elif 'case_title' in df.columns and 'type' not in df.columns:
            # For old system, use case_title as type if type doesn't exist
            df['type'] = df['case_title']

        existing_cols = {k: v for k, v in column_mapping.items() if k in df.columns}
        df = df.rename(columns=existing_cols)

        # =====================================================================
        # Old System: Merge closure_reason_old into reason if reason is empty
        # =====================================================================
        if self.use_old_system and 'closure_reason_old' in df.columns:
            if 'reason' not in df.columns:
                df['reason'] = df['closure_reason_old']
            else:
                # Merge: use reason if it exists, otherwise use closure_reason_old
                df['reason'] = df['reason'].fillna(df['closure_reason_old'])
            # Drop the temporary column
            df = df.drop(columns=['closure_reason_old'])

        # =====================================================================
        # Data Type Conversions
        # =====================================================================
        
        # Convert datetime columns
        datetime_cols = ['open_dt', 'target_dt', 'closed_dt']
        for col in datetime_cols:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')
        
        # Convert numeric columns
        if 'latitude' in df.columns:
            df['latitude'] = pd.to_numeric(df['latitude'], errors='coerce')
        if 'longitude' in df.columns:
            df['longitude'] = pd.to_numeric(df['longitude'], errors='coerce')
        
        # =====================================================================
        # Data Validation
        # =====================================================================
        
        initial_count = len(df)
        
        # Drop records without case ID
        if 'case_enquiry_id' in df.columns:
            df = df[df['case_enquiry_id'].notna()]
            logger.info(
                f"Removed {initial_count - len(df)} records without case_enquiry_id"
            )
        
        # Drop records without open date
        if 'open_dt' in df.columns:
            pre_filter = len(df)
            df = df[df['open_dt'].notna()]
            if pre_filter != len(df):
                logger.info(
                    f"Removed {pre_filter - len(df)} records with invalid open_dt"
                )
        
        # Validate coordinates (but don't drop - some requests may not have location)
        if 'latitude' in df.columns and 'longitude' in df.columns:
            valid_coords = df.apply(
                lambda row: (
                    pd.notna(row['latitude']) and 
                    pd.notna(row['longitude']) and
                    validate_boston_coordinates(row['latitude'], row['longitude'])
                ),
                axis=1
            )
            invalid_count = (~valid_coords).sum()
            if invalid_count > 0:
                logger.info(
                    f"Found {invalid_count} records with missing/invalid coordinates (keeping them)"
                )
        
        # =====================================================================
        # Create PostGIS Geography Points
        # =====================================================================
        
        if 'latitude' in df.columns and 'longitude' in df.columns:
            df['location'] = df.apply(
                lambda row: self.create_geography_point(
                    row['latitude'], 
                    row['longitude']
                ) if pd.notna(row['latitude']) and pd.notna(row['longitude']) else None,
                axis=1
            )
        
        # =====================================================================
        # Add Metadata
        # =====================================================================
        
        from datetime import datetime, timezone
        current_time = datetime.now(timezone.utc)
        
        if 'created_at' not in df.columns:
            df['created_at'] = current_time
        if 'updated_at' not in df.columns:
            df['updated_at'] = current_time
        
        # =====================================================================
        # Select Columns
        # =====================================================================
        
        model = self.get_model()
        db_columns = [c.name for c in model.__table__.columns]
        final_columns = [col for col in db_columns if col in df.columns]
        df = df[final_columns]
        
        logger.info(f"Cleaned data: {len(df)} valid records")
        
        return df


# Convenience instances
service_requests_connector = ServiceRequestsConnector()  # NEW system (default)
service_requests_connector_old = ServiceRequestsConnector(use_old_system=True)  # OLD system (legacy)


if __name__ == "__main__":
    import logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    connector = ServiceRequestsConnector()
    
    print("\n" + "="*70)
    print("Testing 311 Service Requests Connector")
    print("="*70)
    
    print("\n📥 Fetching 50 records...")
    df = connector.fetch_data(limit=50)
    print(f"✅ Fetched {len(df)} records")
    
    print("\n🧹 Cleaning data...")
    df_clean = connector.clean_data(df)
    print(f"✅ Cleaned {len(df_clean)} records")
    
    if not df_clean.empty:
        print("\n📊 Sample data:")
        print(df_clean[['case_enquiry_id', 'case_title', 'case_status', 'open_dt']].head())
    
    print("\n" + "="*70)


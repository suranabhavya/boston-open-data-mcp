"""
Building Violations Dataset Connector

Fetches and processes Boston building code violations.
Updated weekly from Boston Open Data Portal.

Dataset: Building Violations
Source: https://data.boston.gov/dataset/building-violations
"""

import logging
from typing import Optional
import pandas as pd

from datasets.base import BaseDatasetConnector, validate_boston_coordinates
from db.models import BuildingViolation

logger = logging.getLogger(__name__)


class BuildingViolationsConnector(BaseDatasetConnector):
    """
    Connector for Boston building violations data.
    
    Handles fetching, cleaning, and loading building code violations.
    """
    
    # Boston Open Data resource ID for building violations
    RESOURCE_ID = "800a2663-1d6a-46e7-9356-bedb70f5332c"
    
    def __init__(self):
        super().__init__(
            resource_id=self.RESOURCE_ID,
            table_name="building_violations"
        )
    
    def fetch_recent(
        self, 
        limit: int = 500,
        clean: bool = True
    ) -> pd.DataFrame:
        """
        Fetch most recent building violations sorted by status date.
        
        Args:
            limit: Number of recent records to fetch
            clean: Whether to clean the data
            
        Returns:
            DataFrame with recent building violations
        """
        logger.info(f"Fetching {limit} most recent building violations...")
        
        # Use SQL sort to get truly recent violations
        df = self.fetch_data(
            limit=limit,
            sort_field="status_dttm",
            sort_order="DESC"
        )
        
        if clean and not df.empty:
            df = self.clean_data(df)
        
        return df
    
    def get_model(self):
        """Return the BuildingViolation SQLAlchemy model."""
        return BuildingViolation
    
    def clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean and transform building violations data.
        
        Args:
            df: Raw pandas DataFrame from API
            
        Returns:
            Cleaned pandas DataFrame ready for database
        """
        logger.info(f"Cleaning {len(df)} building violation records...")
        
        df = df.copy()
        
        # =====================================================================
        # Column Mapping
        # =====================================================================
        column_mapping = {
            'case_no': 'case_no',
            'status': 'status',
            'status_dttm': 'status_dttm',
            'code': 'code',
            'description': 'description',
            'address': 'address',
            'ward': 'ward',
            'sam_id': 'sam_id',
            'value': 'value',
            'latitude': 'latitude',
            'longitude': 'longitude',
        }
        
        existing_cols = {k: v for k, v in column_mapping.items() if k in df.columns}
        df = df.rename(columns=existing_cols)
        
        # =====================================================================
        # Data Type Conversions
        # =====================================================================
        
        # Convert status_dttm to datetime
        if 'status_dttm' in df.columns:
            df['status_dttm'] = pd.to_datetime(df['status_dttm'], errors='coerce')
            # Filter out invalid dates (year > 9999 causes PostgreSQL errors)
            if df['status_dttm'].notna().any():
                invalid_dates = df['status_dttm'] > pd.Timestamp('9999-12-31')
                invalid_count = invalid_dates.sum()
                if invalid_count > 0:
                    logger.info(f"Setting {invalid_count} invalid future dates to None")
                    df.loc[invalid_dates, 'status_dttm'] = None
        
        # Convert numeric columns
        if 'value' in df.columns:
            df['value'] = pd.to_numeric(df['value'], errors='coerce')
        if 'latitude' in df.columns:
            df['latitude'] = pd.to_numeric(df['latitude'], errors='coerce')
        if 'longitude' in df.columns:
            df['longitude'] = pd.to_numeric(df['longitude'], errors='coerce')
        
        # =====================================================================
        # Data Validation
        # =====================================================================
        
        initial_count = len(df)
        
        # Drop records without case number
        if 'case_no' in df.columns:
            df = df[df['case_no'].notna()]
            removed_no_case = initial_count - len(df)
            if removed_no_case > 0:
                logger.info(f"Removed {removed_no_case} records without case_no")
        
        # Remove duplicates (keep most recent by status_dttm)
        if 'case_no' in df.columns and 'status_dttm' in df.columns:
            before_dedup = len(df)
            df = df.sort_values('status_dttm', ascending=False).drop_duplicates(
                subset=['case_no'], keep='first'
            )
            removed_dupes = before_dedup - len(df)
            if removed_dupes > 0:
                logger.info(f"Removed {removed_dupes} duplicate case_no records")
        
        # Validate coordinates (but don't drop - some violations may not have location)
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


# Convenience instance
building_violations_connector = BuildingViolationsConnector()


if __name__ == "__main__":
    import logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    connector = BuildingViolationsConnector()
    
    print("\n" + "="*70)
    print("Testing Building Violations Connector")
    print("="*70)
    
    print("\n📥 Fetching 50 records...")
    df = connector.fetch_data(limit=50, sort_field="status_dttm", sort_order="DESC")
    print(f"✅ Fetched {len(df)} records")
    
    print("\n🧹 Cleaning data...")
    df_clean = connector.clean_data(df)
    print(f"✅ Cleaned {len(df_clean)} records")
    
    if not df_clean.empty:
        print("\n📊 Sample data:")
        print(df_clean[['case_no', 'status', 'code', 'status_dttm']].head())
    
    print("\n" + "="*70)


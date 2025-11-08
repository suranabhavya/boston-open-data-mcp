"""
Crime Incidents Dataset Connector

Fetches and processes Boston Police Department crime incident reports.
Updated daily from Boston Open Data Portal.

Dataset: Crime Incident Reports (August 2015 to Date)
Source: https://data.boston.gov/dataset/crime-incident-reports-august-2015-to-date-source-new-system
"""

import logging
from typing import Optional
import pandas as pd

from datasets.base import BaseDatasetConnector, validate_boston_coordinates
from db.models import CrimeIncident

logger = logging.getLogger(__name__)


class CrimeIncidentsConnector(BaseDatasetConnector):
    """
    Connector for Boston Police crime incident data.
    
    Handles fetching, cleaning, and loading crime incident reports.
    """
    
    # Boston Open Data resource ID for crime incidents
    RESOURCE_ID = "b973d8cb-eeb2-4e7e-99da-c92938efc9c0"
    
    def __init__(self):
        super().__init__(
            resource_id=self.RESOURCE_ID,
            table_name="crime_incidents"
        )
    
    def get_model(self):
        """Return the CrimeIncident SQLAlchemy model."""
        return CrimeIncident
    
    def clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean and transform crime incident data.
        
        Transformations:
        1. Rename columns from API format to database format
        2. Convert data types
        3. Parse datetime fields
        4. Create PostGIS geography points from lat/lon
        5. Filter invalid records
        6. Add metadata timestamps
        
        Args:
            df: Raw pandas DataFrame from API
            
        Returns:
            Cleaned pandas DataFrame ready for database
        """
        logger.info(f"Cleaning {len(df)} crime records...")
        
        # Create a copy to avoid modifying original
        df = df.copy()
        
        # =====================================================================
        # Column Mapping (API names -> Database names)
        # =====================================================================
        column_mapping = {
            'INCIDENT_NUMBER': 'incident_number',
            'OFFENSE_CODE': 'offense_code',
            'OFFENSE_CODE_GROUP': 'offense_code_group',
            'OFFENSE_DESCRIPTION': 'offense_description',
            'DISTRICT': 'district',
            'REPORTING_AREA': 'reporting_area',
            'SHOOTING': 'shooting',
            'OCCURRED_ON_DATE': 'occurred_on_date',
            'YEAR': 'year',
            'MONTH': 'month',
            'DAY_OF_WEEK': 'day_of_week',
            'HOUR': 'hour',
            'STREET': 'street',
            'Lat': 'latitude',
            'Long': 'longitude',
        }
        
        # Rename columns that exist
        existing_cols = {k: v for k, v in column_mapping.items() if k in df.columns}
        df = df.rename(columns=existing_cols)
        
        # =====================================================================
        # Data Type Conversions
        # =====================================================================
        
        # Convert occurred_on_date to datetime
        if 'occurred_on_date' in df.columns:
            df['occurred_on_date'] = pd.to_datetime(
                df['occurred_on_date'], 
                errors='coerce'
            )
        
        # Convert numeric columns
        numeric_cols = {
            'offense_code': 'Int64',  # Nullable integer
            'year': 'Int64',
            'month': 'Int64',
            'hour': 'Int64',
            'latitude': 'float64',
            'longitude': 'float64',
        }
        
        for col, dtype in numeric_cols.items():
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').astype(dtype)
        
        # Convert shooting to boolean
        if 'shooting' in df.columns:
            df['shooting'] = df['shooting'].map({
                'Y': True,
                'y': True,
                '1': True,
                1: True,
                'N': False,
                'n': False,
                '0': False,
                0: False,
                None: False
            }).fillna(False)
        else:
            df['shooting'] = False
        
        # =====================================================================
        # Data Validation and Filtering
        # =====================================================================
        
        initial_count = len(df)
        
        # Drop records without incident number
        if 'incident_number' in df.columns:
            df = df[df['incident_number'].notna()]
            logger.info(
                f"Removed {initial_count - len(df)} records without incident_number"
            )
        
        # Drop records without valid datetime
        if 'occurred_on_date' in df.columns:
            pre_filter = len(df)
            df = df[df['occurred_on_date'].notna()]
            if pre_filter != len(df):
                logger.info(
                    f"Removed {pre_filter - len(df)} records with invalid dates"
                )
        
        # Validate coordinates
        if 'latitude' in df.columns and 'longitude' in df.columns:
            pre_filter = len(df)
            
            # Check for non-null
            df = df[df['latitude'].notna() & df['longitude'].notna()]
            
            # Validate Boston bounds
            valid_coords = df.apply(
                lambda row: validate_boston_coordinates(
                    row['latitude'], 
                    row['longitude']
                ),
                axis=1
            )
            df = df[valid_coords]
            
            if pre_filter != len(df):
                logger.info(
                    f"Removed {pre_filter - len(df)} records with invalid coordinates"
                )
        
        # =====================================================================
        # Create PostGIS Geography Points
        # =====================================================================
        
        if 'latitude' in df.columns and 'longitude' in df.columns:
            df['location'] = df.apply(
                lambda row: self.create_geography_point(
                    row['latitude'], 
                    row['longitude']
                ),
                axis=1
            )
        
        # =====================================================================
        # Add Metadata
        # =====================================================================
        
        from datetime import datetime
        current_time = datetime.utcnow()
        
        if 'created_at' not in df.columns:
            df['created_at'] = current_time
        if 'updated_at' not in df.columns:
            df['updated_at'] = current_time
        
        # =====================================================================
        # Select and Order Columns
        # =====================================================================
        
        # Select only the columns we need (ignore extra columns from API)
        model = self.get_model()
        db_columns = [c.name for c in model.__table__.columns]
        
        # Keep only columns that exist in both DataFrame and model
        final_columns = [col for col in db_columns if col in df.columns]
        df = df[final_columns]
        
        logger.info(f"Cleaned data: {len(df)} valid records")
        
        return df
    
    def fetch_recent(
        self, 
        limit: int = 500,
        clean: bool = True
    ) -> pd.DataFrame:
        """
        Fetch most recent crime incidents sorted by occurrence date.
        
        Args:
            limit: Number of recent records to fetch
            clean: Whether to clean the data
            
        Returns:
            DataFrame with recent crime incidents
        """
        logger.info(f"Fetching {limit} most recent crimes...")
        
        # Use SQL sort to get truly recent crimes
        df = self.fetch_data(
            limit=limit,
            sort_field="OCCURRED_ON_DATE",
            sort_order="DESC"
        )
        
        if clean and not df.empty:
            df = self.clean_data(df)
        
        return df
    
    def get_summary_stats(self) -> dict:
        """
        Get summary statistics from the database.
        
        Returns:
            Dictionary with statistics
        """
        from db.connection import get_db_session
        from sqlalchemy import func
        
        with get_db_session() as session:
            model = self.get_model()
            
            # Total records
            total = session.query(func.count(model.incident_number)).scalar()
            
            # Date range
            min_date = session.query(func.min(model.occurred_on_date)).scalar()
            max_date = session.query(func.max(model.occurred_on_date)).scalar()
            
            # Top offense types
            top_offenses = session.query(
                model.offense_code_group,
                func.count(model.incident_number).label('count')
            ).group_by(
                model.offense_code_group
            ).order_by(
                func.count(model.incident_number).desc()
            ).limit(10).all()
            
            # Shootings count
            shootings = session.query(
                func.count(model.incident_number)
            ).filter(
                model.shooting == True
            ).scalar()
            
        return {
            'total_incidents': total,
            'date_range': {
                'earliest': min_date.isoformat() if min_date else None,
                'latest': max_date.isoformat() if max_date else None,
            },
            'shootings': shootings,
            'top_offense_types': [
                {'offense': offense, 'count': count}
                for offense, count in top_offenses
            ]
        }


# =============================================================================
# Convenience instance
# =============================================================================

# Create a singleton instance for easy import
crime_connector = CrimeIncidentsConnector()


# =============================================================================
# CLI Testing
# =============================================================================

if __name__ == "__main__":
    # Test the connector
    import logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    connector = CrimeIncidentsConnector()
    
    print("\n" + "="*70)
    print("Testing Crime Incidents Connector")
    print("="*70)
    
    # Fetch a small sample
    print("\n Fetching 100 records...")
    df = connector.fetch_data(limit=100)
    print(f" Fetched {len(df)} records")
    
    # Clean the data
    print("\n Cleaning data...")
    df_clean = connector.clean_data(df)
    print(f" Cleaned {len(df_clean)} records")
    
    print("\n Sample data:")
    print(df_clean[['incident_number', 'offense_code_group', 'occurred_on_date']].head())
    
    print("\n" + "="*70)


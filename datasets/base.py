"""
Base Dataset Connector

This module provides the abstract base class for all dataset connectors.
Each dataset connector is responsible for:
1. Fetching data from Boston's Open Data API
2. Cleaning and transforming the data
3. Storing it in the database

The base class handles common functionality like:
- HTTP requests with retry logic
- Rate limiting
- Error handling
- Progress logging
"""

import logging
from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional
from datetime import datetime

import pandas as pd
import requests
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type
)
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session
from geoalchemy2.shape import from_shape
from shapely.geometry import Point

from config.settings import settings
from db.connection import get_db_session

# Configure logging
logger = logging.getLogger(__name__)


class BaseDatasetConnector(ABC):
    """
    Abstract base class for dataset connectors.
    
    Each dataset connector must implement:
    - fetch_data(): Fetch raw data from API
    - clean_data(): Clean and transform the data
    - get_model(): Return the SQLAlchemy model class
    """
    
    def __init__(self, resource_id: str, table_name: str):
        """
        Initialize the connector.
        
        Args:
            resource_id: Boston Open Data API resource identifier
            table_name: Database table name
        """
        self.resource_id = resource_id
        self.table_name = table_name
        self.api_base_url = settings.boston_data_api_base_url
        self.max_records = settings.max_records_per_request
        
    @abstractmethod
    def get_model(self):
        """
        Return the SQLAlchemy model class for this dataset.
        
        Returns:
            SQLAlchemy model class
        """
        pass
    
    @abstractmethod
    def clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean and transform the raw data.
        
        Args:
            df: Raw pandas DataFrame
            
        Returns:
            Cleaned pandas DataFrame
        """
        pass
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type(requests.exceptions.RequestException)
    )
    def _make_api_request(
        self, 
        endpoint: str, 
        params: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Make an API request with retry logic.
        
        Args:
            endpoint: API endpoint (e.g., 'datastore_search')
            params: Query parameters
            
        Returns:
            JSON response as dictionary
            
        Raises:
            requests.exceptions.RequestException: If request fails after retries
        """
        url = f"{self.api_base_url}/{endpoint}"
        
        logger.debug(f"Making API request to: {url}")
        logger.debug(f"Parameters: {params}")
        
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        
        return response.json()
    
    def fetch_data(
        self, 
        limit: Optional[int] = None,
        offset: int = 0,
        filters: Optional[Dict[str, Any]] = None,
        sort_field: Optional[str] = None,
        sort_order: str = "DESC"
    ) -> pd.DataFrame:
        """
        Fetch data from Boston Open Data API.
        
        Args:
            limit: Maximum number of records to fetch (None = all)
            offset: Starting offset for pagination
            filters: Additional filters for the API query
            sort_field: Field to sort by (uses SQL query if provided)
            sort_order: Sort order - "DESC" or "ASC"
            
        Returns:
            pandas DataFrame with raw data
        """
        all_records = []
        
        logger.info(f"Fetching data for {self.table_name}...")
        
        # If sort_field is provided, use SQL query for proper sorting
        if sort_field:
            return self._fetch_with_sql_sort(limit, sort_field, sort_order, filters)
        
        # Otherwise use regular datastore_search
        current_offset = offset
        fetch_limit = limit or self.max_records
        
        while True:
            # Build API parameters
            params = {
                "resource_id": self.resource_id,
                "limit": min(fetch_limit, self.max_records),
                "offset": current_offset
            }
            
            # Add filters if provided
            if filters:
                params.update(filters)
            
            try:
                # Make API request
                response_data = self._make_api_request("datastore_search", params)
                
                # Extract records
                records = response_data.get("result", {}).get("records", [])
                
                if not records:
                    logger.info(f"No more records found at offset {current_offset}")
                    break
                
                all_records.extend(records)
                logger.info(
                    f"Fetched {len(records)} records "
                    f"(total: {len(all_records)})"
                )
                
                # Check if we should continue
                if limit and len(all_records) >= limit:
                    all_records = all_records[:limit]
                    break
                
                # Check if there are more records
                total = response_data.get("result", {}).get("total", 0)
                if current_offset + len(records) >= total:
                    logger.info(f"Reached end of dataset (total: {total})")
                    break
                
                # Update offset for next iteration
                current_offset += len(records)
                
            except requests.exceptions.RequestException as e:
                logger.error(f"API request failed: {e}")
                if all_records:
                    logger.warning(
                        f"Returning {len(all_records)} records fetched so far"
                    )
                    break
                raise
        
        logger.info(f"Fetched total of {len(all_records)} records")
        
        # Convert to DataFrame
        if all_records:
            return pd.DataFrame(all_records)
        else:
            return pd.DataFrame()
    
    def _fetch_with_sql_sort(
        self,
        limit: Optional[int],
        sort_field: str,
        sort_order: str = "DESC",
        filters: Optional[Dict[str, Any]] = None
    ) -> pd.DataFrame:
        """
        Fetch data using SQL query for proper sorting.
        
        Args:
            limit: Maximum number of records
            sort_field: Field to sort by
            sort_order: Sort order (DESC or ASC)
            filters: Optional WHERE clause filters
            
        Returns:
            pandas DataFrame with sorted data
        """
        logger.info(f"Fetching data with SQL sort by {sort_field} {sort_order}...")
        
        # Build SQL query
        sql = f'SELECT * FROM "{self.resource_id}"'
        
        # Add WHERE clause if filters provided
        if filters:
            where_clauses = []
            for key, value in filters.items():
                where_clauses.append(f'"{key}" = \'{value}\'')
            if where_clauses:
                sql += " WHERE " + " AND ".join(where_clauses)
        
        # Add ORDER BY
        sql += f' ORDER BY "{sort_field}" {sort_order}'
        
        # Add LIMIT
        if limit:
            sql += f' LIMIT {limit}'
        
        logger.info(f"SQL Query: {sql}")
        
        try:
            # Make SQL API request
            params = {"sql": sql}
            response_data = self._make_api_request("datastore_search_sql", params)
            
            # Extract records
            records = response_data.get("result", {}).get("records", [])
            
            logger.info(f"Fetched {len(records)} records via SQL query")
            
            if records:
                return pd.DataFrame(records)
            else:
                return pd.DataFrame()
                
        except requests.exceptions.RequestException as e:
            logger.error(f"SQL query failed: {e}")
            raise
    
    def create_geography_point(
        self, 
        latitude: float, 
        longitude: float
    ) -> Optional[Any]:
        """
        Create a PostGIS geography point from latitude and longitude.
        
        Args:
            latitude: Latitude coordinate
            longitude: Longitude coordinate
            
        Returns:
            WKB element for PostGIS geography, or None if invalid
        """
        try:
            if pd.notna(latitude) and pd.notna(longitude):
                # Validate coordinates
                if -90 <= latitude <= 90 and -180 <= longitude <= 180:
                    point = Point(longitude, latitude)
                    return from_shape(point, srid=4326)
        except Exception as e:
            logger.warning(f"Failed to create geography point: {e}")
        
        return None
    
    def load_data(
        self, 
        df: pd.DataFrame, 
        batch_size: int = 1000,
        upsert: bool = True
    ) -> int:
        """
        Load data into the database.
        
        Args:
            df: pandas DataFrame to load
            batch_size: Number of records per batch
            upsert: If True, use upsert (insert or update), else insert only
            
        Returns:
            Number of records loaded
        """
        if df.empty:
            logger.warning("No data to load")
            return 0
        
        total_records = len(df)
        records_loaded = 0
        
        logger.info(f"Loading {total_records} records into {self.table_name}...")
        
        model = self.get_model()
        
        with get_db_session() as session:
            # Process in batches
            for i in range(0, total_records, batch_size):
                batch = df.iloc[i:i + batch_size]
                records = batch.to_dict('records')
                
                try:
                    if upsert:
                        # Use PostgreSQL INSERT ... ON CONFLICT DO UPDATE
                        stmt = insert(model).values(records)
                        stmt = stmt.on_conflict_do_update(
                            index_elements=[model.__table__.primary_key.columns.keys()[0]],
                            set_={
                                c.name: c 
                                for c in stmt.excluded 
                                if c.name != 'created_at'
                            }
                        )
                        session.execute(stmt)
                    else:
                        # Simple insert
                        session.bulk_insert_mappings(model, records)
                    
                    session.commit()
                    records_loaded += len(records)
                    
                    if records_loaded % (batch_size * 5) == 0:
                        logger.info(
                            f"Loaded {records_loaded}/{total_records} records"
                        )
                    
                except Exception as e:
                    session.rollback()
                    logger.error(f"Failed to load batch {i}: {e}")
                    # Continue with next batch
        
        logger.info(f"Successfully loaded {records_loaded} records")
        return records_loaded
    
    def fetch_and_load(
        self, 
        limit: Optional[int] = None,
        clean: bool = True,
        upsert: bool = True
    ) -> int:
        """
        Fetch data from API, clean it, and load into database.
        
        This is the main method to use for ETL.
        
        Args:
            limit: Maximum number of records to fetch
            clean: Whether to clean the data
            upsert: Whether to use upsert (vs insert)
            
        Returns:
            Number of records loaded
        """
        logger.info(f"Starting ETL for {self.table_name}...")
        start_time = datetime.now()
        
        try:
            # Fetch data
            df = self.fetch_data(limit=limit)
            
            if df.empty:
                logger.warning("No data fetched")
                return 0
            
            logger.info(f"Fetched {len(df)} records")
            
            # Clean data
            if clean:
                df = self.clean_data(df)
                logger.info(f"Cleaned data: {len(df)} records remaining")
            
            # Load data
            records_loaded = self.load_data(df, upsert=upsert)
            
            duration = (datetime.now() - start_time).total_seconds()
            logger.info(
                f"ETL completed for {self.table_name}: "
                f"{records_loaded} records in {duration:.2f}s"
            )
            
            return records_loaded
            
        except Exception as e:
            logger.error(f"ETL failed for {self.table_name}: {e}")
            raise
    
    def get_record_count(self) -> int:
        """
        Get the current number of records in the database.
        
        Returns:
            Number of records
        """
        model = self.get_model()
        
        with get_db_session() as session:
            count = session.query(model).count()
        
        return count


# =============================================================================
# Helper functions
# =============================================================================

def validate_boston_coordinates(
    latitude: float, 
    longitude: float
) -> bool:
    """
    Check if coordinates are within Boston's bounds.
    
    Args:
        latitude: Latitude coordinate
        longitude: Longitude coordinate
        
    Returns:
        True if coordinates are valid for Boston area
    """
    # Boston bounds (approximate)
    MIN_LAT = 42.22
    MAX_LAT = 42.42
    MIN_LON = -71.19
    MAX_LON = -70.99
    
    return (
        MIN_LAT <= latitude <= MAX_LAT and
        MIN_LON <= longitude <= MAX_LON
    )


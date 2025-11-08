"""
Database Models for Boston Open Data

This module defines SQLAlchemy ORM models for all Boston Open Data datasets.
Each model represents a table in the boston_data schema.

Models:
- CrimeIncident: Boston Police crime incident reports
- ServiceRequest: 311 non-emergency service requests
- BuildingViolation: Building code violations
- FoodInspection: Food establishment inspections

All models include PostGIS geography columns for spatial queries.
"""

from datetime import datetime, date
from typing import Optional

from sqlalchemy import (
    Column, String, Integer, Float, Boolean, 
    DateTime, Date, Text, Index
)
from sqlalchemy.ext.declarative import declarative_base
from geoalchemy2 import Geography

from config.settings import settings

# Create declarative base with schema
Base = declarative_base()
Base.metadata.schema = settings.database_schema


# ============================================================================
# Crime Incidents Model
# ============================================================================

class CrimeIncident(Base):
    """
    Boston Police Department crime incident reports.
    
    Updated daily from Boston Open Data Portal.
    Includes offense details, location, and temporal information.
    """
    __tablename__ = "crime_incidents"
    
    # Primary key
    incident_number = Column(String(50), primary_key=True, index=True)
    
    # Offense information
    offense_code = Column(Integer)
    offense_code_group = Column(String(100), index=True)
    offense_description = Column(Text)
    
    # Location information
    district = Column(String(10), index=True)
    reporting_area = Column(String(10))
    street = Column(String(200))
    
    # Temporal information
    occurred_on_date = Column(DateTime, index=True)
    year = Column(Integer, index=True)
    month = Column(Integer, index=True)
    day_of_week = Column(String(20))
    hour = Column(Integer)
    
    # Incident flags
    shooting = Column(Boolean, default=False, index=True)
    
    # Geographic coordinates
    latitude = Column(Float)
    longitude = Column(Float)
    location = Column(Geography(geometry_type='POINT', srid=4326))
    
    # Metadata
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Indexes for common queries
    __table_args__ = (
        Index('idx_crime_location', 'latitude', 'longitude'),
        Index('idx_crime_date_district', 'occurred_on_date', 'district'),
        Index('idx_crime_offense_year', 'offense_code_group', 'year'),
    )
    
    def __repr__(self):
        return (
            f"<CrimeIncident(incident={self.incident_number}, "
            f"offense={self.offense_code_group}, "
            f"date={self.occurred_on_date})>"
        )


# ============================================================================
# Service Requests Model (311)
# ============================================================================

class ServiceRequest(Base):
    """
    311 non-emergency service requests.
    
    Includes sanitation, pest control, street maintenance, and complaints.
    Updated daily from Boston Open Data Portal.
    """
    __tablename__ = "service_requests"
    
    # Primary key
    case_enquiry_id = Column(String(50), primary_key=True, index=True)
    
    # Case information
    case_status = Column(String(20), index=True)
    case_title = Column(String(200), index=True)
    subject = Column(Text)
    reason = Column(String(200))
    type = Column(String(100), index=True)
    department = Column(String(100))
    
    # Temporal information
    open_dt = Column(DateTime, index=True)
    target_dt = Column(DateTime)
    closed_dt = Column(DateTime)
    
    # Location information
    address = Column(String(300))
    ward = Column(String(10))
    neighborhood = Column(String(100), index=True)
    zipcode = Column(String(10))
    
    # Geographic coordinates
    latitude = Column(Float)
    longitude = Column(Float)
    location = Column(Geography(geometry_type='POINT', srid=4326))
    
    # Media
    submittedphoto = Column(String(500))
    closedphoto = Column(String(500))
    
    # Metadata
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Indexes
    __table_args__ = (
        Index('idx_service_location', 'latitude', 'longitude'),
        Index('idx_service_status_type', 'case_status', 'type'),
        Index('idx_service_neighborhood', 'neighborhood', 'open_dt'),
    )
    
    def __repr__(self):
        return (
            f"<ServiceRequest(id={self.case_enquiry_id}, "
            f"type={self.case_title}, "
            f"status={self.case_status})>"
        )
    
    @property
    def is_open(self) -> bool:
        """Check if case is still open."""
        return self.case_status and self.case_status.lower() == 'open'
    
    @property
    def resolution_time_hours(self) -> Optional[float]:
        """Calculate resolution time in hours."""
        if self.open_dt and self.closed_dt:
            delta = self.closed_dt - self.open_dt
            return delta.total_seconds() / 3600
        return None


# ============================================================================
# Building Violations Model
# ============================================================================

class BuildingViolation(Base):
    """
    Building code violations and enforcement actions.
    
    Includes maintenance violations, unsafe conditions, and compliance.
    Updated weekly from Boston Open Data Portal.
    """
    __tablename__ = "building_violations"
    
    # Primary key
    case_no = Column(String(50), primary_key=True, index=True)
    
    # Violation information
    status = Column(String(50), index=True)
    status_dttm = Column(DateTime, index=True)
    code = Column(String(50), index=True)
    description = Column(Text)
    
    # Property information
    address = Column(String(300))
    ward = Column(String(10))
    sam_id = Column(String(50))  # Property identifier
    value = Column(Float)  # Assessed value
    
    # Geographic coordinates
    latitude = Column(Float)
    longitude = Column(Float)
    location = Column(Geography(geometry_type='POINT', srid=4326))
    
    # Metadata
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Indexes
    __table_args__ = (
        Index('idx_violation_location', 'latitude', 'longitude'),
        Index('idx_violation_status_code', 'status', 'code'),
        Index('idx_violation_ward', 'ward', 'status_dttm'),
    )
    
    def __repr__(self):
        return (
            f"<BuildingViolation(case={self.case_no}, "
            f"code={self.code}, "
            f"status={self.status})>"
        )


# ============================================================================
# Food Inspections Model
# ============================================================================

class FoodInspection(Base):
    """
    Food establishment health inspections and violations.
    
    Includes hygiene violations, inspection scores, and compliance.
    Updated weekly from Boston Open Data Portal.
    """
    __tablename__ = "food_inspections"
    
    # Primary key (using composite key approach)
    _id = Column(Integer, primary_key=True, autoincrement=False)
    
    # Business information
    businessname = Column(String(300), index=True)
    licenseno = Column(String(50))
    
    # Violation information
    violstatus = Column(String(50), index=True)
    violdesc = Column(Text)
    viollevel = Column(String(50), index=True)
    
    # Inspection information
    statusdate = Column(Date, index=True)
    
    # Location information
    address = Column(String(300))
    city = Column(String(100))
    state = Column(String(10))
    zip = Column(String(10))
    
    # Geographic coordinates
    latitude = Column(Float)
    longitude = Column(Float)
    location = Column(Geography(geometry_type='POINT', srid=4326))
    
    # Metadata
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Indexes
    __table_args__ = (
        Index('idx_food_location', 'latitude', 'longitude'),
        Index('idx_food_business', 'businessname', 'statusdate'),
        Index('idx_food_violation', 'viollevel', 'violstatus'),
    )
    
    def __repr__(self):
        return (
            f"<FoodInspection(business={self.businessname}, "
            f"status={self.violstatus}, "
            f"date={self.statusdate})>"
        )
    
    @property
    def has_violation(self) -> bool:
        """Check if inspection found violations."""
        return (
            self.violstatus and 
            self.violstatus.lower() not in ['pass', 'no violation']
        )


# ============================================================================
# Helper Functions
# ============================================================================

def create_all_tables():
    """
    Create all tables in the database.
    
    This should be called after models are defined to create the schema.
    """
    from db.connection import engine
    
    # Import here to ensure engine is initialized
    Base.metadata.create_all(bind=engine)
    print(f"Created all tables in schema: {settings.database_schema}")


def drop_all_tables():
    """
    Drop all tables in the database.
    
    WARNING: This is destructive! Use only for development/testing.
    """
    from db.connection import engine
    
    if settings.is_production:
        raise RuntimeError("Cannot drop tables in production!")
    
    Base.metadata.drop_all(bind=engine)
    print(f"Dropped all tables from schema: {settings.database_schema}")


# ============================================================================
# Testing
# ============================================================================

if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO)
    
    print("\n" + "="*70)
    print("Database Models")
    print("="*70)
    print(f"Schema: {settings.database_schema}")
    print(f"Models: {len(Base.metadata.tables)}")
    print("\nTables:")
    for table_name in Base.metadata.tables:
        print(f"  - {table_name}")
    
    # Create tables
    print("\n🔨 Creating tables...")
    create_all_tables()
    
    print("\nAll tables created successfully!")
    print("="*70 + "\n")


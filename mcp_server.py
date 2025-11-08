"""
Boston Open Data MCP Server

Main FastAPI application providing MCP-compliant endpoints for Boston's civic data.

Features:
- Crime incident queries
- 311 service request queries
- Geographic/spatial queries with PostGIS
- Auto-generated OpenAPI documentation
- CORS support for web clients

Run with:
    uvicorn mcp_server:app --reload
"""

import logging
from contextlib import asynccontextmanager
from typing import List, Optional

from fastapi import FastAPI, HTTPException, Query, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from sqlalchemy.orm import Session
from sqlalchemy import func, and_, or_
from geoalchemy2.functions import ST_DWithin, ST_MakePoint
from geoalchemy2 import Geography

from config.settings import settings
from db.connection import get_db, initialize_database, check_database_health
from db.models import CrimeIncident, ServiceRequest

# Configure logging
logging.basicConfig(
    level=settings.log_level,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# ============================================================================
# Application Lifecycle
# ============================================================================

@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Application lifespan manager.
    
    Runs on startup and shutdown to initialize/cleanup resources.
    """
    # Startup
    logger.info("🚀 Starting Boston Open Data MCP Server...")
    logger.info(f"Environment: {settings.environment}")
    logger.info(f"Database Schema: {settings.database_schema}")
    
    try:
        # Initialize database
        initialize_database()
        logger.info("✅ Database initialized successfully")
    except Exception as e:
        logger.error(f"❌ Database initialization failed: {e}")
        raise
    
    yield
    
    # Shutdown
    logger.info("👋 Shutting down Boston Open Data MCP Server...")


# ============================================================================
# FastAPI Application
# ============================================================================

app = FastAPI(
    title="Boston Open Data MCP Server",
    description="""
    MCP (Model Context Protocol) Server for Boston's public datasets.
    
    Provides unified access to:
    - Crime incident reports
    - 311 service requests
    - Building violations
    - Food inspections
    
    All endpoints support geographic queries with PostGIS spatial operations.
    """,
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
    lifespan=lifespan
)

# ============================================================================
# CORS Middleware
# ============================================================================

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins_list,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ============================================================================
# Root & Health Endpoints
# ============================================================================

@app.get("/", tags=["Root"])
async def root():
    """Root endpoint with server information."""
    return {
        "name": "Boston Open Data MCP Server",
        "version": "1.0.0",
        "status": "running",
        "environment": settings.environment,
        "docs": "/docs",
        "endpoints": {
            "crime": "/api/crime",
            "services": "/api/services",
            "health": "/health"
        }
    }


@app.get("/health", tags=["Health"])
async def health_check():
    """Health check endpoint."""
    db_healthy = check_database_health()
    
    return {
        "status": "healthy" if db_healthy else "unhealthy",
        "database": "connected" if db_healthy else "disconnected",
        "environment": settings.environment
    }


# ============================================================================
# Crime Incidents Endpoints
# ============================================================================

@app.get("/api/crime/recent", tags=["Crime"])
async def get_recent_crimes(
    lat: Optional[float] = Query(None, description="Latitude coordinate"),
    lon: Optional[float] = Query(None, description="Longitude coordinate"),
    radius: float = Query(0.5, description="Search radius in miles", ge=0.1, le=5.0),
    days: int = Query(30, description="Number of days to look back", ge=1, le=365),
    offense_type: Optional[str] = Query(None, description="Filter by offense type"),
    shooting: Optional[bool] = Query(None, description="Filter shootings only"),
    limit: int = Query(100, description="Maximum results", ge=1, le=1000),
    db: Session = Depends(get_db)
):
    """
    Get recent crime incidents.
    
    Supports geographic filtering by lat/lon + radius.
    """
    from datetime import datetime, timedelta
    
    # Build query
    query = db.query(CrimeIncident)
    
    # Time filter
    cutoff_date = datetime.now() - timedelta(days=days)
    query = query.filter(CrimeIncident.occurred_on_date >= cutoff_date)
    
    # Geographic filter
    if lat is not None and lon is not None:
        # Convert miles to meters
        radius_meters = radius * 1609.34
        
        # Create point and filter by distance
        point = func.ST_MakePoint(lon, lat)
        point_geography = func.cast(point, Geography)
        
        query = query.filter(
            func.ST_DWithin(
                CrimeIncident.location,
                point_geography,
                radius_meters
            )
        )
    
    # Offense type filter
    if offense_type:
        query = query.filter(
            CrimeIncident.offense_code_group.ilike(f"%{offense_type}%")
        )
    
    # Shooting filter
    if shooting is not None:
        query = query.filter(CrimeIncident.shooting == shooting)
    
    # Order by date (most recent first)
    query = query.order_by(CrimeIncident.occurred_on_date.desc())
    
    # Limit results
    query = query.limit(limit)
    
    # Execute query
    results = query.all()
    
    # Format response
    return {
        "total": len(results),
        "filters": {
            "location": {"lat": lat, "lon": lon, "radius_miles": radius} if lat and lon else None,
            "days": days,
            "offense_type": offense_type,
            "shooting": shooting
        },
        "data": [
            {
                "incident_number": r.incident_number,
                "offense_code_group": r.offense_code_group,
                "offense_description": r.offense_description,
                "district": r.district,
                "occurred_on_date": r.occurred_on_date.isoformat() if r.occurred_on_date else None,
                "shooting": r.shooting,
                "street": r.street,
                "location": {
                    "latitude": r.latitude,
                    "longitude": r.longitude
                } if r.latitude and r.longitude else None
            }
            for r in results
        ]
    }


@app.get("/api/crime/stats", tags=["Crime"])
async def get_crime_stats(
    days: int = Query(30, description="Number of days for statistics", ge=1, le=365),
    db: Session = Depends(get_db)
):
    """Get crime statistics for the specified time period."""
    from datetime import datetime, timedelta
    
    cutoff_date = datetime.now() - timedelta(days=days)
    
    # Total crimes
    total = db.query(func.count(CrimeIncident.incident_number)).filter(
        CrimeIncident.occurred_on_date >= cutoff_date
    ).scalar()
    
    # Shootings
    shootings = db.query(func.count(CrimeIncident.incident_number)).filter(
        and_(
            CrimeIncident.occurred_on_date >= cutoff_date,
            CrimeIncident.shooting == True
        )
    ).scalar()
    
    # Top offense types
    top_offenses = db.query(
        CrimeIncident.offense_code_group,
        func.count(CrimeIncident.incident_number).label('count')
    ).filter(
        CrimeIncident.occurred_on_date >= cutoff_date
    ).group_by(
        CrimeIncident.offense_code_group
    ).order_by(
        func.count(CrimeIncident.incident_number).desc()
    ).limit(10).all()
    
    # By district
    by_district = db.query(
        CrimeIncident.district,
        func.count(CrimeIncident.incident_number).label('count')
    ).filter(
        CrimeIncident.occurred_on_date >= cutoff_date
    ).group_by(
        CrimeIncident.district
    ).order_by(
        func.count(CrimeIncident.incident_number).desc()
    ).all()
    
    return {
        "period_days": days,
        "total_incidents": total,
        "shootings": shootings,
        "top_offense_types": [
            {"offense": offense, "count": count}
            for offense, count in top_offenses
        ],
        "by_district": [
            {"district": district, "count": count}
            for district, count in by_district
        ]
    }


# ============================================================================
# 311 Service Requests Endpoints
# ============================================================================

@app.get("/api/services/requests", tags=["311 Services"])
async def get_service_requests(
    lat: Optional[float] = Query(None, description="Latitude coordinate"),
    lon: Optional[float] = Query(None, description="Longitude coordinate"),
    radius: float = Query(0.5, description="Search radius in miles", ge=0.1, le=5.0),
    status: Optional[str] = Query(None, description="Filter by status (Open/Closed)"),
    case_type: Optional[str] = Query(None, description="Filter by case type"),
    neighborhood: Optional[str] = Query(None, description="Filter by neighborhood"),
    days: int = Query(30, description="Number of days to look back", ge=1, le=365),
    limit: int = Query(100, description="Maximum results", ge=1, le=1000),
    db: Session = Depends(get_db)
):
    """
    Get 311 service requests.
    
    Supports filtering by location, status, type, and neighborhood.
    """
    from datetime import datetime, timedelta
    
    # Build query
    query = db.query(ServiceRequest)
    
    # Time filter
    cutoff_date = datetime.now() - timedelta(days=days)
    query = query.filter(ServiceRequest.open_dt >= cutoff_date)
    
    # Geographic filter
    if lat is not None and lon is not None:
        radius_meters = radius * 1609.34
        point = func.ST_MakePoint(lon, lat)
        point_geography = func.cast(point, Geography)
        
        query = query.filter(
            func.ST_DWithin(
                ServiceRequest.location,
                point_geography,
                radius_meters
            )
        )
    
    # Status filter
    if status:
        query = query.filter(ServiceRequest.case_status.ilike(f"%{status}%"))
    
    # Type filter
    if case_type:
        query = query.filter(ServiceRequest.case_title.ilike(f"%{case_type}%"))
    
    # Neighborhood filter
    if neighborhood:
        query = query.filter(ServiceRequest.neighborhood.ilike(f"%{neighborhood}%"))
    
    # Order by date
    query = query.order_by(ServiceRequest.open_dt.desc())
    
    # Limit
    query = query.limit(limit)
    
    # Execute
    results = query.all()
    
    return {
        "total": len(results),
        "filters": {
            "location": {"lat": lat, "lon": lon, "radius_miles": radius} if lat and lon else None,
            "status": status,
            "case_type": case_type,
            "neighborhood": neighborhood,
            "days": days
        },
        "data": [
            {
                "case_enquiry_id": r.case_enquiry_id,
                "case_title": r.case_title,
                "case_status": r.case_status,
                "open_dt": r.open_dt.isoformat() if r.open_dt else None,
                "closed_dt": r.closed_dt.isoformat() if r.closed_dt else None,
                "neighborhood": r.neighborhood,
                "address": r.address,
                "location": {
                    "latitude": r.latitude,
                    "longitude": r.longitude
                } if r.latitude and r.longitude else None
            }
            for r in results
        ]
    }


# ============================================================================
# Error Handlers
# ============================================================================

@app.exception_handler(404)
async def not_found_handler(request, exc):
    """Custom 404 handler."""
    return JSONResponse(
        status_code=404,
        content={
            "error": "Not Found",
            "message": "The requested resource was not found",
            "path": str(request.url)
        }
    )


@app.exception_handler(500)
async def internal_error_handler(request, exc):
    """Custom 500 handler."""
    logger.error(f"Internal server error: {exc}")
    return JSONResponse(
        status_code=500,
        content={
            "error": "Internal Server Error",
            "message": "An unexpected error occurred"
        }
    )


# ============================================================================
# Development Server
# ============================================================================

if __name__ == "__main__":
    import uvicorn
    
    uvicorn.run(
        "mcp_server:app",
        host=settings.api_host,
        port=settings.api_port,
        reload=settings.api_reload,
        log_level=settings.log_level.lower()
    )


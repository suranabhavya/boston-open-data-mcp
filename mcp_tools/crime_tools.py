"""
Crime Data Tools for MCP Server

These tools allow LLMs to query Boston crime incident data.
Each tool is defined with:
1. Tool definition (name, description, input schema)
2. Handler function (executes the query and formats results)
"""

import logging
from typing import Any, Dict, List, Optional
from datetime import datetime, timedelta
from sqlalchemy import func, and_, or_
from db.connection import get_db_session
from db.models import CrimeIncident

logger = logging.getLogger(__name__)


# ============================================================================
# Tool 1: Get Recent Crimes
# ============================================================================

def get_recent_crimes_tool():
    """
    Tool definition for getting recent crime incidents.
    
    This returns the tool metadata that tells Claude:
    - What the tool is called
    - What it does
    - What parameters it accepts
    """
    return {
        "name": "get_recent_crimes",
        "description": (
            "Retrieve recent crime incidents from Boston. "
            "Returns details including offense type, location, date, time, and coordinates. "
            "Useful for answering questions about recent crime activity, safety concerns, "
            "or specific types of incidents. Data is updated daily from Boston Police Department."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "limit": {
                    "type": "integer",
                    "description": "Maximum number of records to return (default: 10, max: 100)",
                    "default": 10,
                    "minimum": 1,
                    "maximum": 100
                },
                "neighborhood": {
                    "type": "string",
                    "description": (
                        "Filter by Boston neighborhood (e.g., 'Back Bay', 'South Boston', "
                        "'Dorchester', 'Jamaica Plain'). Leave empty for all neighborhoods."
                    )
                },
                "offense_type": {
                    "type": "string",
                    "description": (
                        "Filter by offense category (e.g., 'Motor Vehicle Accident Response', "
                        "'Larceny', 'Drug Violation', 'Assault'). Leave empty for all types."
                    )
                },
                "days": {
                    "type": "integer",
                    "description": "Number of days to look back (default: 7, max: 30)",
                    "default": 7,
                    "minimum": 1,
                    "maximum": 30
                }
            },
            "required": []  # All parameters are optional
        }
    }


async def handle_get_recent_crimes(arguments: Dict[str, Any]) -> str:
    """
    Handler function that executes the get_recent_crimes tool.
    
    This function:
    1. Extracts parameters from arguments
    2. Queries the database
    3. Formats results in an LLM-friendly way
    
    Args:
        arguments: Dictionary of parameters from Claude
        
    Returns:
        Formatted string with crime data
    """
    try:
        # Extract parameters with defaults
        limit = min(arguments.get("limit", 10), 100)
        neighborhood = arguments.get("neighborhood")
        offense_type = arguments.get("offense_type")
        days = min(arguments.get("days", 7), 30)
        
        # Calculate date range
        cutoff_date = datetime.now() - timedelta(days=days)
        
        logger.info(
            f"Fetching recent crimes: limit={limit}, neighborhood={neighborhood}, "
            f"offense_type={offense_type}, days={days}"
        )
        
        # Query database
        with get_db_session() as session:
            query = session.query(CrimeIncident).filter(
                CrimeIncident.occurred_on_date >= cutoff_date
            )
            
            # Apply filters
            if neighborhood:
                query = query.filter(
                    CrimeIncident.district.ilike(f"%{neighborhood}%")
                )
            
            if offense_type:
                query = query.filter(
                    CrimeIncident.offense_code_group.ilike(f"%{offense_type}%")
                )
            
            # Order by most recent and limit
            crimes = query.order_by(
                CrimeIncident.occurred_on_date.desc()
            ).limit(limit).all()
            
            # Format results
            if not crimes:
                return (
                    f"No crime incidents found in the last {days} days"
                    + (f" in {neighborhood}" if neighborhood else "")
                    + (f" for offense type '{offense_type}'" if offense_type else "")
                    + "."
                )
            
            # Build response
            response_lines = [
                f"Found {len(crimes)} crime incident(s) in the last {days} days:\n"
            ]
            
            for i, crime in enumerate(crimes, 1):
                date_str = crime.occurred_on_date.strftime("%Y-%m-%d %H:%M")
                offense = crime.offense_code_group or "Unknown"
                desc = crime.offense_description or "No description"
                location = crime.street or "Location not specified"
                district = crime.district or "Unknown district"
                
                response_lines.append(
                    f"{i}. {offense} - {desc}\n"
                    f"   Date/Time: {date_str}\n"
                    f"   Location: {location}, {district}\n"
                    f"   Incident #: {crime.incident_number}"
                )
                
                if crime.shooting:
                    response_lines.append("   ⚠️  Shooting involved")
                
                response_lines.append("")  # Empty line between incidents
            
            return "\n".join(response_lines)
            
    except Exception as e:
        logger.error(f"Error in get_recent_crimes: {e}")
        return f"Error retrieving crime data: {str(e)}"


# ============================================================================
# Tool 2: Search Crimes by Location
# ============================================================================

def search_crimes_by_location_tool():
    """Tool definition for searching crimes near a specific location."""
    return {
        "name": "search_crimes_by_location",
        "description": (
            "Search for crime incidents near a specific location using coordinates. "
            "Returns crimes within a specified radius. Useful for questions like "
            "'What crimes happened near this address?' or 'Is this area safe?'"
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "latitude": {
                    "type": "number",
                    "description": "Latitude coordinate (e.g., 42.3601 for Boston)"
                },
                "longitude": {
                    "type": "number",
                    "description": "Longitude coordinate (e.g., -71.0589 for Boston)"
                },
                "radius_km": {
                    "type": "number",
                    "description": "Search radius in kilometers (default: 0.5, max: 5)",
                    "default": 0.5,
                    "minimum": 0.1,
                    "maximum": 5
                },
                "limit": {
                    "type": "integer",
                    "description": "Maximum number of results (default: 20, max: 100)",
                    "default": 20,
                    "minimum": 1,
                    "maximum": 100
                },
                "days": {
                    "type": "integer",
                    "description": "Number of days to look back (default: 30)",
                    "default": 30,
                    "minimum": 1,
                    "maximum": 90
                }
            },
            "required": ["latitude", "longitude"]
        }
    }


async def handle_search_crimes_by_location(arguments: Dict[str, Any]) -> str:
    """Handler for searching crimes by location."""
    try:
        latitude = arguments.get("latitude")
        longitude = arguments.get("longitude")
        radius_km = min(arguments.get("radius_km", 0.5), 5)
        limit = min(arguments.get("limit", 20), 100)
        days = min(arguments.get("days", 30), 90)
        
        if latitude is None or longitude is None:
            return "Error: latitude and longitude are required parameters."
        
        cutoff_date = datetime.now() - timedelta(days=days)
        
        logger.info(
            f"Searching crimes near ({latitude}, {longitude}) "
            f"within {radius_km}km, last {days} days"
        )
        
        with get_db_session() as session:
            # Use PostGIS to find crimes within radius
            # ST_DWithin uses meters, so convert km to meters
            radius_meters = radius_km * 1000
            
            crimes = session.query(CrimeIncident).filter(
                and_(
                    CrimeIncident.occurred_on_date >= cutoff_date,
                    CrimeIncident.location.isnot(None),
                    func.ST_DWithin(
                        CrimeIncident.location,
                        func.ST_GeogFromText(f'SRID=4326;POINT({longitude} {latitude})'),
                        radius_meters
                    )
                )
            ).order_by(
                CrimeIncident.occurred_on_date.desc()
            ).limit(limit).all()
            
            if not crimes:
                return (
                    f"No crime incidents found within {radius_km}km of "
                    f"({latitude:.4f}, {longitude:.4f}) in the last {days} days."
                )
            
            response_lines = [
                f"Found {len(crimes)} crime incident(s) within {radius_km}km "
                f"of the specified location:\n"
            ]
            
            for i, crime in enumerate(crimes, 1):
                date_str = crime.occurred_on_date.strftime("%Y-%m-%d %H:%M")
                offense = crime.offense_code_group or "Unknown"
                location = crime.street or "Location not specified"
                
                # Calculate approximate distance
                if crime.latitude and crime.longitude:
                    # Simple distance calculation (not exact but good enough)
                    lat_diff = abs(crime.latitude - latitude)
                    lon_diff = abs(crime.longitude - longitude)
                    approx_dist = ((lat_diff**2 + lon_diff**2)**0.5) * 111  # Rough km conversion
                    dist_str = f" (~{approx_dist:.2f}km away)"
                else:
                    dist_str = ""
                
                response_lines.append(
                    f"{i}. {offense}{dist_str}\n"
                    f"   Date: {date_str}\n"
                    f"   Location: {location}"
                )
                response_lines.append("")
            
            return "\n".join(response_lines)
            
    except Exception as e:
        logger.error(f"Error in search_crimes_by_location: {e}")
        return f"Error searching crimes by location: {str(e)}"


# ============================================================================
# Tool 3: Get Crime Statistics
# ============================================================================

def get_crime_statistics_tool():
    """Tool definition for getting crime statistics and aggregations."""
    return {
        "name": "get_crime_statistics",
        "description": (
            "Get aggregated statistics about crime incidents in Boston. "
            "Returns counts by offense type, neighborhood, or time period. "
            "Useful for questions like 'What are the most common crimes?' or "
            "'Which neighborhood has the most incidents?'"
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "group_by": {
                    "type": "string",
                    "enum": ["offense_type", "neighborhood", "hour", "day_of_week"],
                    "description": "How to group the statistics",
                    "default": "offense_type"
                },
                "days": {
                    "type": "integer",
                    "description": "Number of days to analyze (default: 30)",
                    "default": 30,
                    "minimum": 1,
                    "maximum": 90
                },
                "limit": {
                    "type": "integer",
                    "description": "Number of top results to return (default: 10)",
                    "default": 10,
                    "minimum": 1,
                    "maximum": 50
                }
            },
            "required": []
        }
    }


async def handle_get_crime_statistics(arguments: Dict[str, Any]) -> str:
    """Handler for getting crime statistics."""
    try:
        group_by = arguments.get("group_by", "offense_type")
        days = min(arguments.get("days", 30), 90)
        limit = min(arguments.get("limit", 10), 50)
        
        cutoff_date = datetime.now() - timedelta(days=days)
        
        logger.info(f"Getting crime statistics: group_by={group_by}, days={days}")
        
        with get_db_session() as session:
            # Build query based on grouping
            if group_by == "offense_type":
                query = session.query(
                    CrimeIncident.offense_code_group,
                    func.count(CrimeIncident.incident_number).label('count')
                ).filter(
                    CrimeIncident.occurred_on_date >= cutoff_date
                ).group_by(
                    CrimeIncident.offense_code_group
                ).order_by(
                    func.count(CrimeIncident.incident_number).desc()
                ).limit(limit)
                
                results = query.all()
                
                if not results:
                    return f"No crime statistics available for the last {days} days."
                
                total = sum(count for _, count in results)
                response_lines = [
                    f"Crime Statistics by Offense Type (Last {days} days):\n",
                    f"Total Incidents: {total}\n"
                ]
                
                for i, (offense_type, count) in enumerate(results, 1):
                    percentage = (count / total) * 100
                    response_lines.append(
                        f"{i}. {offense_type or 'Unknown'}: {count} incidents ({percentage:.1f}%)"
                    )
                
            elif group_by == "neighborhood":
                query = session.query(
                    CrimeIncident.district,
                    func.count(CrimeIncident.incident_number).label('count')
                ).filter(
                    CrimeIncident.occurred_on_date >= cutoff_date
                ).group_by(
                    CrimeIncident.district
                ).order_by(
                    func.count(CrimeIncident.incident_number).desc()
                ).limit(limit)
                
                results = query.all()
                total = sum(count for _, count in results)
                
                response_lines = [
                    f"Crime Statistics by District (Last {days} days):\n",
                    f"Total Incidents: {total}\n"
                ]
                
                for i, (district, count) in enumerate(results, 1):
                    percentage = (count / total) * 100
                    response_lines.append(
                        f"{i}. {district or 'Unknown'}: {count} incidents ({percentage:.1f}%)"
                    )
            
            elif group_by == "hour":
                query = session.query(
                    CrimeIncident.hour,
                    func.count(CrimeIncident.incident_number).label('count')
                ).filter(
                    CrimeIncident.occurred_on_date >= cutoff_date
                ).group_by(
                    CrimeIncident.hour
                ).order_by(
                    func.count(CrimeIncident.incident_number).desc()
                ).limit(limit)
                
                results = query.all()
                total = sum(count for _, count in results)
                
                response_lines = [
                    f"Crime Statistics by Hour of Day (Last {days} days):\n",
                    f"Total Incidents: {total}\n"
                ]
                
                for i, (hour, count) in enumerate(results, 1):
                    percentage = (count / total) * 100
                    time_str = f"{hour:02d}:00" if hour is not None else "Unknown"
                    response_lines.append(
                        f"{i}. {time_str}: {count} incidents ({percentage:.1f}%)"
                    )
            
            elif group_by == "day_of_week":
                query = session.query(
                    CrimeIncident.day_of_week,
                    func.count(CrimeIncident.incident_number).label('count')
                ).filter(
                    CrimeIncident.occurred_on_date >= cutoff_date
                ).group_by(
                    CrimeIncident.day_of_week
                ).order_by(
                    func.count(CrimeIncident.incident_number).desc()
                ).limit(limit)
                
                results = query.all()
                total = sum(count for _, count in results)
                
                response_lines = [
                    f"Crime Statistics by Day of Week (Last {days} days):\n",
                    f"Total Incidents: {total}\n"
                ]
                
                for i, (day, count) in enumerate(results, 1):
                    percentage = (count / total) * 100
                    response_lines.append(
                        f"{i}. {day or 'Unknown'}: {count} incidents ({percentage:.1f}%)"
                    )
            
            return "\n".join(response_lines)
            
    except Exception as e:
        logger.error(f"Error in get_crime_statistics: {e}")
        return f"Error retrieving crime statistics: {str(e)}"


"""
311 Service Request Tools for MCP Server

These tools allow LLMs to query Boston 311 service request data.
Includes searches, statistics, and open request tracking.
"""

import logging
from typing import Any, Dict
from datetime import datetime, timedelta
from sqlalchemy import func, and_
from db.connection import get_db_session
from db.models import ServiceRequest

logger = logging.getLogger(__name__)


# ============================================================================
# Tool 1: Search Service Requests
# ============================================================================

def search_service_requests_tool():
    """Tool definition for searching 311 service requests."""
    return {
        "name": "search_service_requests",
        "description": (
            "Search Boston 311 service requests (non-emergency city services). "
            "Includes requests for street repairs, trash pickup, tree maintenance, "
            "animal control, and more. Useful for questions about city service issues, "
            "complaint patterns, or neighborhood maintenance."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "request_type": {
                    "type": "string",
                    "description": (
                        "Filter by request type (e.g., 'Street Light Outage', "
                        "'Pothole', 'Trash Collection', 'Fallen Tree')"
                    )
                },
                "status": {
                    "type": "string",
                    "description": "Filter by status (e.g., 'Open', 'Closed', 'In progress')"
                },
                "neighborhood": {
                    "type": "string",
                    "description": "Filter by Boston neighborhood"
                },
                "days": {
                    "type": "integer",
                    "description": "Number of days to look back (default: 7, max: 90)",
                    "default": 7,
                    "minimum": 1,
                    "maximum": 90
                },
                "limit": {
                    "type": "integer",
                    "description": "Maximum number of results (default: 20, max: 100)",
                    "default": 20,
                    "minimum": 1,
                    "maximum": 100
                }
            },
            "required": []
        }
    }


async def handle_search_service_requests(arguments: Dict[str, Any]) -> str:
    """Handler for searching service requests."""
    try:
        request_type = arguments.get("request_type")
        status = arguments.get("status")
        neighborhood = arguments.get("neighborhood")
        days = min(arguments.get("days", 7), 90)
        limit = min(arguments.get("limit", 20), 100)
        
        cutoff_date = datetime.now() - timedelta(days=days)
        
        logger.info(
            f"Searching service requests: type={request_type}, status={status}, "
            f"neighborhood={neighborhood}, days={days}"
        )
        
        with get_db_session() as session:
            query = session.query(ServiceRequest).filter(
                ServiceRequest.open_dt >= cutoff_date
            )
            
            # Apply filters
            if request_type:
                query = query.filter(
                    ServiceRequest.case_title.ilike(f"%{request_type}%")
                )
            
            if status:
                query = query.filter(
                    ServiceRequest.case_status.ilike(f"%{status}%")
                )
            
            if neighborhood:
                query = query.filter(
                    ServiceRequest.neighborhood.ilike(f"%{neighborhood}%")
                )
            
            # Order by most recent
            requests = query.order_by(
                ServiceRequest.open_dt.desc()
            ).limit(limit).all()
            
            if not requests:
                filters_str = []
                if request_type:
                    filters_str.append(f"type '{request_type}'")
                if status:
                    filters_str.append(f"status '{status}'")
                if neighborhood:
                    filters_str.append(f"in {neighborhood}")
                
                filter_desc = " ".join(filters_str) if filters_str else ""
                return f"No service requests found {filter_desc} in the last {days} days."
            
            # Format response
            response_lines = [
                f"Found {len(requests)} service request(s) in the last {days} days:\n"
            ]
            
            for i, req in enumerate(requests, 1):
                open_date = req.open_dt.strftime("%Y-%m-%d %H:%M") if req.open_dt else "Unknown"
                req_type = req.case_title or "Unknown type"
                req_status = req.case_status or "Unknown status"
                location = req.address or "Location not specified"
                hood = req.neighborhood or "Unknown neighborhood"
                
                response_lines.append(
                    f"{i}. {req_type}\n"
                    f"   Status: {req_status}\n"
                    f"   Opened: {open_date}\n"
                    f"   Location: {location}, {hood}\n"
                    f"   Case ID: {req.case_enquiry_id}"
                )
                
                # Add closure info if closed
                if req.closed_dt:
                    closed_date = req.closed_dt.strftime("%Y-%m-%d")
                    response_lines.append(f"   Closed: {closed_date}")
                
                response_lines.append("")
            
            return "\n".join(response_lines)
            
    except Exception as e:
        logger.error(f"Error in search_service_requests: {e}")
        return f"Error searching service requests: {str(e)}"


# ============================================================================
# Tool 2: Get Service Request Statistics
# ============================================================================

def get_service_request_stats_tool():
    """Tool definition for getting service request statistics."""
    return {
        "name": "get_service_request_stats",
        "description": (
            "Get aggregated statistics about 311 service requests. "
            "Returns counts by request type, neighborhood, or status. "
            "Useful for understanding service patterns and common issues."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "group_by": {
                    "type": "string",
                    "enum": ["request_type", "neighborhood", "status"],
                    "description": "How to group the statistics",
                    "default": "request_type"
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
                    "description": "Number of top results (default: 10)",
                    "default": 10,
                    "minimum": 1,
                    "maximum": 50
                }
            },
            "required": []
        }
    }


async def handle_get_service_request_stats(arguments: Dict[str, Any]) -> str:
    """Handler for getting service request statistics."""
    try:
        group_by = arguments.get("group_by", "request_type")
        days = min(arguments.get("days", 30), 90)
        limit = min(arguments.get("limit", 10), 50)
        
        cutoff_date = datetime.now() - timedelta(days=days)
        
        logger.info(f"Getting service request stats: group_by={group_by}, days={days}")
        
        with get_db_session() as session:
            if group_by == "request_type":
                query = session.query(
                    ServiceRequest.case_title,
                    func.count(ServiceRequest.case_enquiry_id).label('count')
                ).filter(
                    ServiceRequest.open_dt >= cutoff_date
                ).group_by(
                    ServiceRequest.case_title
                ).order_by(
                    func.count(ServiceRequest.case_enquiry_id).desc()
                ).limit(limit)
                
                results = query.all()
                total = sum(count for _, count in results)
                
                response_lines = [
                    f"311 Service Request Statistics by Type (Last {days} days):\n",
                    f"Total Requests: {total}\n"
                ]
                
                for i, (req_type, count) in enumerate(results, 1):
                    percentage = (count / total) * 100 if total > 0 else 0
                    response_lines.append(
                        f"{i}. {req_type or 'Unknown'}: {count} requests ({percentage:.1f}%)"
                    )
            
            elif group_by == "neighborhood":
                query = session.query(
                    ServiceRequest.neighborhood,
                    func.count(ServiceRequest.case_enquiry_id).label('count')
                ).filter(
                    ServiceRequest.open_dt >= cutoff_date
                ).group_by(
                    ServiceRequest.neighborhood
                ).order_by(
                    func.count(ServiceRequest.case_enquiry_id).desc()
                ).limit(limit)
                
                results = query.all()
                total = sum(count for _, count in results)
                
                response_lines = [
                    f"311 Service Request Statistics by Neighborhood (Last {days} days):\n",
                    f"Total Requests: {total}\n"
                ]
                
                for i, (hood, count) in enumerate(results, 1):
                    percentage = (count / total) * 100 if total > 0 else 0
                    response_lines.append(
                        f"{i}. {hood or 'Unknown'}: {count} requests ({percentage:.1f}%)"
                    )
            
            elif group_by == "status":
                query = session.query(
                    ServiceRequest.case_status,
                    func.count(ServiceRequest.case_enquiry_id).label('count')
                ).filter(
                    ServiceRequest.open_dt >= cutoff_date
                ).group_by(
                    ServiceRequest.case_status
                ).order_by(
                    func.count(ServiceRequest.case_enquiry_id).desc()
                ).limit(limit)
                
                results = query.all()
                total = sum(count for _, count in results)
                
                response_lines = [
                    f"311 Service Request Statistics by Status (Last {days} days):\n",
                    f"Total Requests: {total}\n"
                ]
                
                for i, (status, count) in enumerate(results, 1):
                    percentage = (count / total) * 100 if total > 0 else 0
                    response_lines.append(
                        f"{i}. {status or 'Unknown'}: {count} requests ({percentage:.1f}%)"
                    )
            
            return "\n".join(response_lines)
            
    except Exception as e:
        logger.error(f"Error in get_service_request_stats: {e}")
        return f"Error retrieving service request statistics: {str(e)}"


# ============================================================================
# Tool 3: Find Open Requests
# ============================================================================

def find_open_requests_tool():
    """Tool definition for finding unresolved service requests."""
    return {
        "name": "find_open_requests",
        "description": (
            "Find unresolved (open or in-progress) 311 service requests. "
            "Useful for checking on pending issues or finding requests that "
            "have been open for a long time."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "request_type": {
                    "type": "string",
                    "description": "Filter by request type"
                },
                "neighborhood": {
                    "type": "string",
                    "description": "Filter by neighborhood"
                },
                "min_days_open": {
                    "type": "integer",
                    "description": "Minimum days the request has been open (default: 0)",
                    "default": 0,
                    "minimum": 0
                },
                "limit": {
                    "type": "integer",
                    "description": "Maximum number of results (default: 20, max: 100)",
                    "default": 20,
                    "minimum": 1,
                    "maximum": 100
                }
            },
            "required": []
        }
    }


async def handle_find_open_requests(arguments: Dict[str, Any]) -> str:
    """Handler for finding open service requests."""
    try:
        request_type = arguments.get("request_type")
        neighborhood = arguments.get("neighborhood")
        min_days_open = arguments.get("min_days_open", 0)
        limit = min(arguments.get("limit", 20), 100)
        
        cutoff_date = datetime.now() - timedelta(days=min_days_open)
        
        logger.info(
            f"Finding open requests: type={request_type}, neighborhood={neighborhood}, "
            f"min_days_open={min_days_open}"
        )
        
        with get_db_session() as session:
            # Query for open or in-progress requests
            query = session.query(ServiceRequest).filter(
                and_(
                    ServiceRequest.closed_dt.is_(None),  # Not closed
                    ServiceRequest.open_dt <= cutoff_date  # Open for at least min_days
                )
            )
            
            # Apply filters
            if request_type:
                query = query.filter(
                    ServiceRequest.case_title.ilike(f"%{request_type}%")
                )
            
            if neighborhood:
                query = query.filter(
                    ServiceRequest.neighborhood.ilike(f"%{neighborhood}%")
                )
            
            # Order by oldest first
            requests = query.order_by(
                ServiceRequest.open_dt.asc()
            ).limit(limit).all()
            
            if not requests:
                filters_str = []
                if request_type:
                    filters_str.append(f"type '{request_type}'")
                if neighborhood:
                    filters_str.append(f"in {neighborhood}")
                if min_days_open > 0:
                    filters_str.append(f"open for at least {min_days_open} days")
                
                filter_desc = " ".join(filters_str) if filters_str else ""
                return f"No open service requests found {filter_desc}."
            
            # Format response
            response_lines = [
                f"Found {len(requests)} open service request(s):\n"
            ]
            
            for i, req in enumerate(requests, 1):
                open_date = req.open_dt.strftime("%Y-%m-%d") if req.open_dt else "Unknown"
                req_type = req.case_title or "Unknown type"
                req_status = req.case_status or "Open"
                location = req.address or "Location not specified"
                hood = req.neighborhood or "Unknown neighborhood"
                
                # Calculate days open
                if req.open_dt:
                    days_open = (datetime.now() - req.open_dt).days
                    days_str = f" ({days_open} days open)"
                else:
                    days_str = ""
                
                response_lines.append(
                    f"{i}. {req_type}{days_str}\n"
                    f"   Status: {req_status}\n"
                    f"   Opened: {open_date}\n"
                    f"   Location: {location}, {hood}\n"
                    f"   Case ID: {req.case_enquiry_id}"
                )
                response_lines.append("")
            
            return "\n".join(response_lines)
            
    except Exception as e:
        logger.error(f"Error in find_open_requests: {e}")
        return f"Error finding open requests: {str(e)}"


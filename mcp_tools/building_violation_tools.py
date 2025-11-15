"""
Building Violation Tools for MCP Server

These tools allow LLMs to query Boston building code violation data.
Includes searches by status, location, and violation type.
"""

import logging
from typing import Any, Dict
from datetime import datetime, timedelta
from sqlalchemy import func, and_
from db.connection import get_db_session
from db.models import BuildingViolation

logger = logging.getLogger(__name__)


# ============================================================================
# Tool 1: Search Building Violations
# ============================================================================

def search_building_violations_tool():
    """Tool definition for searching building violations."""
    return {
        "name": "search_building_violations",
        "description": (
            "Search Boston building code violations and enforcement actions. "
            "Includes maintenance violations, unsafe conditions, and compliance issues. "
            "Useful for questions about building safety, property violations, or "
            "code enforcement patterns."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "status": {
                    "type": "string",
                    "description": "Filter by status ('Open' or 'Closed')"
                },
                "code": {
                    "type": "string",
                    "description": "Filter by violation code (e.g., 'BI-1', 'BI-2')"
                },
                "description": {
                    "type": "string",
                    "description": "Search in violation description (e.g., 'unsafe', 'fire')"
                },
                "neighborhood": {
                    "type": "string",
                    "description": "Filter by neighborhood or ward"
                },
                "days": {
                    "type": "integer",
                    "description": "Number of days to look back (default: 30, max: 365)",
                    "default": 30,
                    "minimum": 1,
                    "maximum": 365
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


async def handle_search_building_violations(arguments: Dict[str, Any]) -> str:
    """Handler for searching building violations."""
    try:
        status = arguments.get("status")
        code = arguments.get("code")
        description = arguments.get("description")
        neighborhood = arguments.get("neighborhood")
        days = min(arguments.get("days", 30), 365)
        limit = min(arguments.get("limit", 20), 100)
        
        cutoff_date = datetime.now() - timedelta(days=days)
        
        logger.info(
            f"Searching building violations: status={status}, code={code}, "
            f"description={description}, days={days}"
        )
        
        with get_db_session() as session:
            query = session.query(BuildingViolation).filter(
                BuildingViolation.status_dttm >= cutoff_date
            )
            
            # Apply filters
            if status:
                query = query.filter(
                    BuildingViolation.status.ilike(f"%{status}%")
                )
            
            if code:
                query = query.filter(
                    BuildingViolation.code.ilike(f"%{code}%")
                )
            
            if description:
                query = query.filter(
                    BuildingViolation.description.ilike(f"%{description}%")
                )
            
            if neighborhood:
                query = query.filter(
                    BuildingViolation.ward.ilike(f"%{neighborhood}%")
                )
            
            # Order by most recent
            violations = query.order_by(
                BuildingViolation.status_dttm.desc()
            ).limit(limit).all()
            
            if not violations:
                filters_str = []
                if status:
                    filters_str.append(f"status '{status}'")
                if code:
                    filters_str.append(f"code '{code}'")
                if description:
                    filters_str.append(f"description containing '{description}'")
                if neighborhood:
                    filters_str.append(f"in {neighborhood}")
                
                filter_desc = " ".join(filters_str) if filters_str else ""
                return f"No building violations found {filter_desc} in the last {days} days."
            
            # Format response
            response_lines = [
                f"Found {len(violations)} building violation(s) in the last {days} days:\n"
            ]
            
            for i, viol in enumerate(violations, 1):
                status_date = viol.status_dttm.strftime("%Y-%m-%d") if viol.status_dttm else "Unknown"
                viol_status = viol.status or "Unknown"
                viol_code = viol.code or "No code"
                viol_desc = viol.description or "No description"
                address = viol.address or "Address not specified"
                ward = viol.ward or "Unknown ward"
                
                response_lines.append(
                    f"{i}. {viol_code}: {viol_desc}\n"
                    f"   Status: {viol_status}\n"
                    f"   Status Date: {status_date}\n"
                    f"   Address: {address}\n"
                    f"   Ward: {ward}\n"
                    f"   Case #: {viol.case_no}"
                )
                
                # Add value if present
                if viol.value:
                    response_lines.append(f"   Value: ${viol.value:,.2f}")
                
                response_lines.append("")
            
            return "\n".join(response_lines)
            
    except Exception as e:
        logger.error(f"Error in search_building_violations: {e}")
        return f"Error searching building violations: {str(e)}"


# ============================================================================
# Tool 2: Get Violations by Status
# ============================================================================

def get_violations_by_status_tool():
    """Tool definition for getting violations grouped by status."""
    return {
        "name": "get_violations_by_status",
        "description": (
            "Get building violations grouped by their status (Open vs Closed). "
            "Returns counts and statistics. Useful for understanding the current "
            "state of building code enforcement and backlog."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "include_details": {
                    "type": "boolean",
                    "description": "Include sample violations for each status (default: false)",
                    "default": False
                },
                "days": {
                    "type": "integer",
                    "description": "Number of days to analyze (default: 90, max: 365)",
                    "default": 90,
                    "minimum": 1,
                    "maximum": 365
                },
                "sample_size": {
                    "type": "integer",
                    "description": "Number of sample violations per status (default: 5)",
                    "default": 5,
                    "minimum": 1,
                    "maximum": 20
                }
            },
            "required": []
        }
    }


async def handle_get_violations_by_status(arguments: Dict[str, Any]) -> str:
    """Handler for getting violations by status."""
    try:
        include_details = arguments.get("include_details", False)
        days = min(arguments.get("days", 90), 365)
        sample_size = min(arguments.get("sample_size", 5), 20)
        
        cutoff_date = datetime.now() - timedelta(days=days)
        
        logger.info(f"Getting violations by status: days={days}, include_details={include_details}")
        
        with get_db_session() as session:
            # Get counts by status
            status_counts = session.query(
                BuildingViolation.status,
                func.count(BuildingViolation.case_no).label('count')
            ).filter(
                BuildingViolation.status_dttm >= cutoff_date
            ).group_by(
                BuildingViolation.status
            ).order_by(
                func.count(BuildingViolation.case_no).desc()
            ).all()
            
            if not status_counts:
                return f"No building violations found in the last {days} days."
            
            total = sum(count for _, count in status_counts)
            
            response_lines = [
                f"Building Violations by Status (Last {days} days):\n",
                f"Total Violations: {total}\n"
            ]
            
            for status, count in status_counts:
                percentage = (count / total) * 100 if total > 0 else 0
                response_lines.append(
                    f"• {status or 'Unknown'}: {count} violations ({percentage:.1f}%)"
                )
            
            # Add sample violations if requested
            if include_details:
                response_lines.append("\n" + "="*60)
                
                for status, _ in status_counts:
                    response_lines.append(f"\nSample {status or 'Unknown'} Violations:\n")
                    
                    # Get sample violations for this status
                    samples = session.query(BuildingViolation).filter(
                        and_(
                            BuildingViolation.status_dttm >= cutoff_date,
                            BuildingViolation.status == status
                        )
                    ).order_by(
                        BuildingViolation.status_dttm.desc()
                    ).limit(sample_size).all()
                    
                    for i, viol in enumerate(samples, 1):
                        status_date = viol.status_dttm.strftime("%Y-%m-%d") if viol.status_dttm else "Unknown"
                        viol_code = viol.code or "No code"
                        viol_desc = viol.description or "No description"
                        address = viol.address or "Address not specified"
                        
                        response_lines.append(
                            f"  {i}. {viol_code}: {viol_desc}\n"
                            f"     Date: {status_date} | Address: {address}"
                        )
            
            return "\n".join(response_lines)
            
    except Exception as e:
        logger.error(f"Error in get_violations_by_status: {e}")
        return f"Error retrieving violations by status: {str(e)}"


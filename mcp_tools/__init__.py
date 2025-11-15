"""
MCP Tools Package

This package contains tool definitions and handlers for the Boston Open Data MCP Server.
Each module defines tools that Claude (or other LLMs) can call to query Boston's public datasets.

Modules:
- crime_tools: Tools for querying crime incident data
- service_request_tools: Tools for querying 311 service requests
- building_violation_tools: Tools for querying building code violations
"""

# Tool definitions and handlers will be imported by the MCP server
# This package organizes all MCP tools for Boston Open Data

__all__ = [
    "crime_tools",
    "service_request_tools",
    "building_violation_tools",
]


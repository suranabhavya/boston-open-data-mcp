#!/usr/bin/env python3
"""
Boston Open Data MCP Protocol Server

This server implements the Model Context Protocol (MCP) to allow LLMs like Claude
to query Boston's public datasets through natural language.

The server:
1. Communicates via stdio (stdin/stdout) using JSON-RPC 2.0
2. Registers tools that Claude can discover and call
3. Executes tool requests and returns formatted results
4. Handles errors gracefully

Usage:
    python mcp_protocol_server.py

Configuration:
    Set environment variables in .env file:
    - DATABASE_URL: PostgreSQL connection string
    - DATABASE_SCHEMA: Schema name (default: boston_data)
"""

import asyncio
import logging
import sys
from typing import Any, Dict

# MCP SDK imports
from mcp.server import Server
from mcp.server.stdio import stdio_server
from mcp.types import Tool, TextContent

# Our tool imports
from mcp_tools.crime_tools import (
    get_recent_crimes_tool,
    handle_get_recent_crimes,
    search_crimes_by_location_tool,
    handle_search_crimes_by_location,
    get_crime_statistics_tool,
    handle_get_crime_statistics,
)

from mcp_tools.service_request_tools import (
    search_service_requests_tool,
    handle_search_service_requests,
    get_service_request_stats_tool,
    handle_get_service_request_stats,
    find_open_requests_tool,
    handle_find_open_requests,
)

from mcp_tools.building_violation_tools import (
    search_building_violations_tool,
    handle_search_building_violations,
    get_violations_by_status_tool,
    handle_get_violations_by_status,
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/tmp/boston_mcp_server.log'),  # Log to file
        logging.StreamHandler(sys.stderr)  # Also log to stderr (not stdout!)
    ]
)
logger = logging.getLogger(__name__)


# ============================================================================
# MCP Server Setup
# ============================================================================

# Create the MCP server instance
app = Server("boston-open-data")

logger.info("Boston Open Data MCP Server initializing...")


# ============================================================================
# Tool Registry
# ============================================================================

# Map tool names to their handler functions
TOOL_HANDLERS = {
    # Crime tools
    "get_recent_crimes": handle_get_recent_crimes,
    "search_crimes_by_location": handle_search_crimes_by_location,
    "get_crime_statistics": handle_get_crime_statistics,
    
    # Service request tools
    "search_service_requests": handle_search_service_requests,
    "get_service_request_stats": handle_get_service_request_stats,
    "find_open_requests": handle_find_open_requests,
    
    # Building violation tools
    "search_building_violations": handle_search_building_violations,
    "get_violations_by_status": handle_get_violations_by_status,
}


# ============================================================================
# MCP Protocol Handlers
# ============================================================================

@app.list_tools()
async def list_tools() -> list[Tool]:
    """
    Handle the 'tools/list' request from Claude.
    
    This is called when Claude wants to know what tools are available.
    We return a list of all tool definitions with their schemas.
    
    Returns:
        List of Tool objects that Claude can call
    """
    logger.info("Client requested tool list")
    
    tools = [
        # Crime tools
        Tool(
            name=get_recent_crimes_tool()["name"],
            description=get_recent_crimes_tool()["description"],
            inputSchema=get_recent_crimes_tool()["inputSchema"]
        ),
        Tool(
            name=search_crimes_by_location_tool()["name"],
            description=search_crimes_by_location_tool()["description"],
            inputSchema=search_crimes_by_location_tool()["inputSchema"]
        ),
        Tool(
            name=get_crime_statistics_tool()["name"],
            description=get_crime_statistics_tool()["description"],
            inputSchema=get_crime_statistics_tool()["inputSchema"]
        ),
        
        # Service request tools
        Tool(
            name=search_service_requests_tool()["name"],
            description=search_service_requests_tool()["description"],
            inputSchema=search_service_requests_tool()["inputSchema"]
        ),
        Tool(
            name=get_service_request_stats_tool()["name"],
            description=get_service_request_stats_tool()["description"],
            inputSchema=get_service_request_stats_tool()["inputSchema"]
        ),
        Tool(
            name=find_open_requests_tool()["name"],
            description=find_open_requests_tool()["description"],
            inputSchema=find_open_requests_tool()["inputSchema"]
        ),
        
        # Building violation tools
        Tool(
            name=search_building_violations_tool()["name"],
            description=search_building_violations_tool()["description"],
            inputSchema=search_building_violations_tool()["inputSchema"]
        ),
        Tool(
            name=get_violations_by_status_tool()["name"],
            description=get_violations_by_status_tool()["description"],
            inputSchema=get_violations_by_status_tool()["inputSchema"]
        ),
    ]
    
    logger.info(f"Returning {len(tools)} tools to client")
    return tools


@app.call_tool()
async def call_tool(name: str, arguments: Dict[str, Any]) -> list[TextContent]:
    """
    Handle the 'tools/call' request from Claude.
    
    This is called when Claude wants to execute a specific tool.
    We look up the handler, execute it, and return the results.
    
    Args:
        name: The name of the tool to call
        arguments: Dictionary of parameters for the tool
        
    Returns:
        List containing a TextContent with the tool's response
    """
    logger.info(f"Client called tool: {name} with arguments: {arguments}")
    
    # Check if tool exists
    if name not in TOOL_HANDLERS:
        error_msg = f"Unknown tool: {name}"
        logger.error(error_msg)
        return [TextContent(type="text", text=error_msg)]
    
    try:
        # Get the handler function
        handler = TOOL_HANDLERS[name]
        
        # Execute the handler
        result = await handler(arguments)
        
        logger.info(f"Tool {name} executed successfully")
        
        # Return the result wrapped in TextContent
        return [TextContent(type="text", text=result)]
        
    except Exception as e:
        error_msg = f"Error executing tool {name}: {str(e)}"
        logger.error(error_msg, exc_info=True)
        return [TextContent(type="text", text=error_msg)]


# ============================================================================
# Server Lifecycle
# ============================================================================

async def main():
    """
    Main entry point for the MCP server.
    
    This function:
    1. Sets up the stdio transport (reads from stdin, writes to stdout)
    2. Starts the server
    3. Handles the connection lifecycle
    """
    logger.info("Starting Boston Open Data MCP Server...")
    logger.info(f"Registered {len(TOOL_HANDLERS)} tools")
    
    try:
        # Run the server with stdio transport
        # This means:
        # - Read JSON-RPC requests from stdin
        # - Write JSON-RPC responses to stdout
        # - Log to stderr (not stdout!)
        async with stdio_server() as (read_stream, write_stream):
            logger.info("Server ready and waiting for requests")
            await app.run(
                read_stream,
                write_stream,
                app.create_initialization_options()
            )
    except Exception as e:
        logger.error(f"Server error: {e}", exc_info=True)
        sys.exit(1)


# ============================================================================
# Entry Point
# ============================================================================

if __name__ == "__main__":
    """
    Run the server when executed directly.
    
    This will be called by Claude Desktop or other MCP clients.
    """
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Server stopped by user")
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        sys.exit(1)


# Contributing to Boston Open Data MCP Server

Thank you for your interest in contributing to the Boston Open Data MCP Server!

## Getting Started

1. **Fork the repository** and clone it locally
2. **Install dependencies**: `pip install -r requirements.txt`
3. **Set up your environment**: Copy `.env.example` to `.env` and configure your database connection
4. **Run the initial data load**: `python scripts/load_initial_data.py`

## Development Workflow

### Adding New Datasets

1. Create a new connector in `datasets/` following the pattern of existing connectors
2. Extend from `BaseDatasetConnector`
3. Define the data model in `db/models.py`
4. Create MCP tool handlers in `mcp_tools/`
5. Register the tools in `mcp_protocol_server.py`

### Adding New MCP Tools

1. Create tool definition and handler in the appropriate `mcp_tools/*.py` file
2. Follow the existing pattern:
   - Tool definition function (returns tool metadata)
   - Handler function (executes the query)
3. Register in `TOOL_HANDLERS` dict in `mcp_protocol_server.py`

## Code Style

- Follow PEP 8 for Python code
- Use type hints where appropriate
- Add docstrings to all functions and classes
- Keep functions focused and single-purpose

## Testing

Before submitting:

1. Test your changes with the MCP protocol server
2. Verify data loads correctly
3. Ensure all existing tools still work

## Submitting Changes

1. Create a new branch for your feature/fix
2. Make your changes with clear commit messages
3. Push to your fork
4. Submit a pull request with a clear description

## Questions?

Open an issue on GitHub if you have questions or need help.

## License

By contributing, you agree that your contributions will be licensed under the same license as the project.

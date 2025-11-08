"""
Boston Open Data MCP Server - Configuration Settings

This module loads configuration from environment variables using Pydantic Settings.
It provides type-safe access to all configuration values and validates them on startup.

Key Features:
- Type-safe configuration with validation
- Automatic loading from .env file
- Default values for optional settings
- Clear error messages if required values are missing
"""

from typing import List
from pydantic import Field, PostgresDsn, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """
    Application settings loaded from environment variables.
    
    All settings can be overridden by environment variables.
    The .env file is automatically loaded if present.
    """
    
    # ========================================================================
    # Database Configuration
    # ========================================================================
    database_url: PostgresDsn = Field(
        ...,  # Required field
        description="PostgreSQL connection string from NeonDB"
    )
    
    database_schema: str = Field(
        default="boston_data",
        description="Database schema for organizing tables"
    )
    
    # ========================================================================
    # Application Configuration
    # ========================================================================
    environment: str = Field(
        default="development",
        description="Environment: development, staging, or production"
    )
    
    api_host: str = Field(
        default="0.0.0.0",
        description="Host to bind the API server to"
    )
    
    api_port: int = Field(
        default=8000,
        description="Port to bind the API server to"
    )
    
    api_reload: bool = Field(
        default=True,
        description="Auto-reload on code changes (dev only)"
    )
    
    log_level: str = Field(
        default="INFO",
        description="Logging level: DEBUG, INFO, WARNING, ERROR, CRITICAL"
    )
    
    # ========================================================================
    # API Configuration
    # ========================================================================
    boston_data_api_base_url: str = Field(
        default="https://data.boston.gov/api/3/action",
        description="Boston Open Data API base URL"
    )
    
    mbta_api_key: str = Field(
        default="",
        description="MBTA API key (optional, for real-time transit data)"
    )
    
    # ========================================================================
    # Data Refresh Configuration
    # ========================================================================
    data_refresh_interval_hours: int = Field(
        default=24,
        description="How often to refresh data from Boston's APIs (in hours)"
    )
    
    max_records_per_request: int = Field(
        default=10000,
        description="Maximum records to fetch per API call"
    )
    
    # ========================================================================
    # CORS Configuration
    # ========================================================================
    cors_origins: str = Field(
        default="http://localhost:3000,http://localhost:8000",
        description="Comma-separated list of allowed CORS origins"
    )
    
    # ========================================================================
    # Model Configuration
    # ========================================================================
    model_config = SettingsConfigDict(
        env_file=".env",  # Load from .env file
        env_file_encoding="utf-8",
        case_sensitive=False,  # Allow case-insensitive env vars
        extra="ignore"  # Ignore extra env vars
    )
    
    # ========================================================================
    # Computed Properties
    # ========================================================================
    @property
    def cors_origins_list(self) -> List[str]:
        """Parse CORS origins string into a list."""
        return [origin.strip() for origin in self.cors_origins.split(",")]
    
    @property
    def is_development(self) -> bool:
        """Check if running in development mode."""
        return self.environment.lower() == "development"
    
    @property
    def is_production(self) -> bool:
        """Check if running in production mode."""
        return self.environment.lower() == "production"
    
    @property
    def database_url_str(self) -> str:
        """Get database URL as string."""
        return str(self.database_url)
    
    # ========================================================================
    # Validators
    # ========================================================================
    @field_validator("log_level")
    @classmethod
    def validate_log_level(cls, v: str) -> str:
        """Ensure log level is valid."""
        allowed = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
        v_upper = v.upper()
        if v_upper not in allowed:
            raise ValueError(f"log_level must be one of {allowed}")
        return v_upper
    
    @field_validator("environment")
    @classmethod
    def validate_environment(cls, v: str) -> str:
        """Ensure environment is valid."""
        allowed = ["development", "staging", "production"]
        v_lower = v.lower()
        if v_lower not in allowed:
            raise ValueError(f"environment must be one of {allowed}")
        return v_lower


# ============================================================================
# Global Settings Instance
# ============================================================================
# This will be imported throughout the application
# It's instantiated once and reused everywhere

try:
    settings = Settings()
except Exception as e:
    print(f"Error loading configuration: {e}")
    print("Make sure you have a .env file with DATABASE_URL set")
    raise


# ============================================================================
# Helper function for debugging
# ============================================================================
def print_settings() -> None:
    """Print current settings (useful for debugging)."""
    print("\n" + "="*70)
    print("🔧 Boston MCP Server Configuration")
    print("="*70)
    print(f"Environment:        {settings.environment}")
    print(f"API Host:           {settings.api_host}:{settings.api_port}")
    print(f"Log Level:          {settings.log_level}")
    print(f"Database Schema:    {settings.database_schema}")
    print(f"Database Connected: (NeonDB)")
    print(f"CORS Origins:       {len(settings.cors_origins_list)} origin(s)")
    print(f"Refresh Interval:   {settings.data_refresh_interval_hours} hours")
    print("="*70 + "\n")


# Print settings on import (only in development)
if __name__ == "__main__":
    print_settings()


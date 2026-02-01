"""
Custom exception classes for the OpenDB MCP Server.
"""

from typing import Optional


class DatabaseError(Exception):
    """Base exception for database-related errors."""

    def __init__(
        self,
        message: str,
        source_id: str,
        original_error: Optional[Exception] = None,
    ):
        super().__init__(message)
        self.source_id = source_id
        self.original_error = original_error


class ConnectionError(DatabaseError):
    """Exception for connection failures."""

    def __init__(self, source_id: str, original_error: Optional[Exception] = None):
        message = f"Failed to connect to database: {original_error or 'Unknown error'}"
        super().__init__(message, source_id, original_error)


class QueryError(DatabaseError):
    """Exception for query execution failures."""

    def __init__(
        self,
        source_id: str,
        query: str,
        original_error: Optional[Exception] = None,
    ):
        message = f"Query execution failed: {original_error or 'Unknown error'}"
        super().__init__(message, source_id, original_error)
        self.query = query


class ConfigurationError(Exception):
    """Exception for configuration errors."""

    pass


class KerberosError(Exception):
    """Exception for Kerberos authentication errors."""

    def __init__(self, message: str, original_error: Optional[Exception] = None):
        super().__init__(message)
        self.original_error = original_error


def format_error_for_response(error: Exception) -> str:
    """Format an error for MCP tool response."""
    if isinstance(error, DatabaseError):
        return f"Database Error ({error.source_id}): {error}"

    if isinstance(error, ConfigurationError):
        return f"Configuration Error: {error}"

    if isinstance(error, KerberosError):
        return f"Kerberos Authentication Error: {error}"

    return f"Error: {error}"

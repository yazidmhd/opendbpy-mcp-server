"""
Shared constants for the OpenDB MCP Server.
"""

import re

# Maximum characters in response to prevent memory issues
CHARACTER_LIMIT = 100_000

# Default maximum rows returned from queries
DEFAULT_MAX_ROWS = 1000

# Default query timeout in seconds
DEFAULT_QUERY_TIMEOUT = 30

# Default connection timeout in seconds
DEFAULT_CONNECTION_TIMEOUT = 10

# Server name and version
SERVER_NAME = "opendb-mcp-server"
SERVER_VERSION = "1.0.0"

# Environment variable pattern for config substitution
# Format: ${VAR_NAME} or ${VAR_NAME:-default}
ENV_VAR_PATTERN = re.compile(r"\$\{([^}]+)\}")

# Default ports for various databases
DEFAULT_PORTS: dict[str, int] = {
    "postgres": 5432,
    "mysql": 3306,
    "mariadb": 3306,
    "sqlite": 0,  # SQLite uses file paths
    "hive": 10000,
    "impala": 21050,
}

# Database types supported
DATABASE_TYPES = ("postgres", "mysql", "mariadb", "sqlite", "hive", "impala")

# Authentication mechanisms for Hive/Impala
AUTH_MECHANISMS = ("NONE", "PLAIN", "KERBEROS")

# Object types for schema search
OBJECT_TYPES = ("schema", "table", "column", "index", "procedure")

# Response formats
RESPONSE_FORMATS = ("markdown", "json")

# Write operation keywords for read-only enforcement
WRITE_KEYWORDS = (
    "INSERT",
    "UPDATE",
    "DELETE",
    "DROP",
    "CREATE",
    "ALTER",
    "TRUNCATE",
    "GRANT",
    "REVOKE",
    "MERGE",
    "UPSERT",
    "REPLACE",
)

"""
Abstract base connector class.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Literal, Optional

from ..config.types import SourceConfig
from ..constants import DEFAULT_MAX_ROWS, DEFAULT_QUERY_TIMEOUT, WRITE_KEYWORDS
from ..utils.errors import QueryError
from ..utils.formatters import QueryResult, SchemaObject
from ..utils.logger import logger


@dataclass
class ConnectorOptions:
    """Options for database connectors."""

    readonly: bool = False
    max_rows: int = DEFAULT_MAX_ROWS
    query_timeout: Optional[int] = DEFAULT_QUERY_TIMEOUT
    connection_timeout: Optional[int] = None


@dataclass
class ExecuteOptions:
    """Options for query execution."""

    params: Optional[list[Any]] = None
    max_rows: Optional[int] = None
    timeout: Optional[int] = None


@dataclass
class SchemaSearchOptions:
    """Options for schema object search."""

    object_type: Optional[Literal["schema", "table", "column", "index", "procedure"]] = None
    schema: Optional[str] = None
    table: Optional[str] = None
    pattern: Optional[str] = None


class BaseConnector(ABC):
    """Abstract base class for database connectors."""

    def __init__(self, config: SourceConfig, options: Optional[ConnectorOptions] = None):
        self._config = config
        self._options = options or ConnectorOptions()
        self._is_connected = False

    @property
    def source_id(self) -> str:
        """Get the source ID."""
        return self._config.id

    @property
    @abstractmethod
    def db_type(self) -> str:
        """Get the database type."""
        pass

    @property
    def is_connected(self) -> bool:
        """Check if connected to the database."""
        return self._is_connected

    @property
    def options(self) -> ConnectorOptions:
        """Get connector options."""
        return self._options

    @abstractmethod
    async def connect(self) -> None:
        """Connect to the database."""
        pass

    @abstractmethod
    async def disconnect(self) -> None:
        """Disconnect from the database."""
        pass

    async def execute(self, sql: str, options: Optional[ExecuteOptions] = None) -> QueryResult:
        """Execute a SQL query."""
        if not self._is_connected:
            raise QueryError(self.source_id, sql, Exception("Not connected to database"))

        # Enforce read-only mode
        if self._options.readonly and self._is_write_query(sql):
            raise QueryError(
                self.source_id,
                sql,
                Exception("Write operations are not allowed in read-only mode"),
            )

        opts = options or ExecuteOptions()
        max_rows = opts.max_rows or self._options.max_rows
        timeout = opts.timeout or self._options.query_timeout

        logger.debug(f"Executing query on {self.source_id}", {"sql": sql[:200]})

        return await self._execute_query(sql, opts.params, max_rows, timeout)

    @abstractmethod
    async def _execute_query(
        self,
        sql: str,
        params: Optional[list[Any]],
        max_rows: int,
        timeout: Optional[int],
    ) -> QueryResult:
        """Execute a query (implementation)."""
        pass

    @abstractmethod
    async def search_objects(
        self, options: Optional[SchemaSearchOptions] = None
    ) -> list[SchemaObject]:
        """Search for database objects."""
        pass

    async def test_connection(self) -> bool:
        """Test the database connection."""
        try:
            await self.execute("SELECT 1")
            return True
        except Exception:
            return False

    def _is_write_query(self, sql: str) -> bool:
        """Check if a query is a write operation."""
        normalized = sql.strip().upper()
        for keyword in WRITE_KEYWORDS:
            if normalized.startswith(keyword):
                return True
        return False

    def _wrap_with_limit(self, sql: str, max_rows: int) -> str:
        """Wrap a query with row limiting."""
        normalized = sql.strip().upper()

        # Don't wrap if already has LIMIT/TOP/FETCH
        if " LIMIT " in normalized or " TOP " in normalized or " FETCH " in normalized:
            return sql

        # Don't wrap non-SELECT statements
        if not normalized.startswith("SELECT"):
            return sql

        return f"{sql.strip()} LIMIT {max_rows}"

    def _format_rows(
        self, rows: list[dict[str, Any]], max_rows: int
    ) -> tuple[list[dict[str, Any]], bool]:
        """Format result rows and check for truncation."""
        truncated = len(rows) > max_rows
        limited_rows = rows[:max_rows] if truncated else rows
        return limited_rows, truncated

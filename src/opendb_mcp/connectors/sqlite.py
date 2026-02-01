"""
SQLite connector using aiosqlite.
"""

from pathlib import Path
from typing import Any, Optional

from ..config.types import SqliteSourceConfig, SourceConfig
from ..utils.errors import ConnectionError, QueryError
from ..utils.formatters import QueryResult, SchemaObject
from ..utils.logger import logger
from .base import BaseConnector, ConnectorOptions, SchemaSearchOptions

try:
    import aiosqlite
except ImportError:
    aiosqlite = None  # type: ignore


class SqliteConnector(BaseConnector):
    """SQLite database connector using aiosqlite."""

    def __init__(self, config: SourceConfig, options: Optional[ConnectorOptions] = None):
        super().__init__(config, options)
        self._db: Optional[aiosqlite.Connection] = None

    @property
    def db_type(self) -> str:
        return "sqlite"

    async def connect(self) -> None:
        if self._is_connected:
            return

        if aiosqlite is None:
            raise ImportError(
                "aiosqlite is required for SQLite. Install with: pip install aiosqlite"
            )

        try:
            config = self._config
            if not isinstance(config, SqliteSourceConfig):
                raise ValueError("Invalid SQLite configuration")

            db_path = config.path
            is_memory = db_path == ":memory:"

            if not is_memory:
                resolved_path = Path(db_path).resolve()
                if not resolved_path.exists():
                    raise FileNotFoundError(f"SQLite database file not found: {resolved_path}")
                db_path = str(resolved_path)

            # Open connection
            # Note: aiosqlite doesn't support readonly mode directly in the same way
            # We enforce readonly through SQL validation instead
            self._db = await aiosqlite.connect(db_path)

            # Enable row factory to get dict-like rows
            self._db.row_factory = aiosqlite.Row

            self._is_connected = True
            logger.info(f"Connected to SQLite: {self.source_id}")

        except Exception as e:
            raise ConnectionError(self.source_id, e) from e

    async def disconnect(self) -> None:
        if self._db:
            await self._db.close()
            self._db = None
            self._is_connected = False
            logger.info(f"Disconnected from SQLite: {self.source_id}")

    async def _execute_query(
        self,
        sql: str,
        params: Optional[list[Any]],
        max_rows: int,
        timeout: Optional[int],
    ) -> QueryResult:
        if not self._db:
            raise QueryError(self.source_id, sql, Exception("Not connected"))

        try:
            normalized = sql.strip().upper()
            is_select = normalized.startswith("SELECT") or normalized.startswith("WITH")

            if is_select:
                wrapped_sql = self._wrap_with_limit(sql, max_rows + 1)

                cursor = await self._db.execute(wrapped_sql, params or [])
                rows = await cursor.fetchall()

                if not rows:
                    return QueryResult(columns=[], rows=[], row_count=0, truncated=False)

                # Get column names from cursor description
                columns = [desc[0] for desc in cursor.description] if cursor.description else []

                # Convert rows to dicts
                row_dicts = [dict(row) for row in rows]
                formatted_rows, truncated = self._format_rows(row_dicts, max_rows)

                return QueryResult(
                    columns=columns,
                    rows=formatted_rows,
                    row_count=len(rows),
                    truncated=truncated,
                )
            else:
                # Non-SELECT query
                cursor = await self._db.execute(sql, params or [])
                await self._db.commit()

                return QueryResult(
                    columns=["changes", "lastRowId"],
                    rows=[{"changes": cursor.rowcount, "lastRowId": cursor.lastrowid}],
                    row_count=cursor.rowcount,
                    truncated=False,
                )

        except Exception as e:
            raise QueryError(self.source_id, sql, e) from e

    async def search_objects(
        self, options: Optional[SchemaSearchOptions] = None
    ) -> list[SchemaObject]:
        if not self._db:
            raise Exception("Not connected")

        opts = options or SchemaSearchOptions()
        objects: list[SchemaObject] = []
        pattern = opts.pattern or "%"

        # SQLite doesn't have schemas, but we still support the interface
        if not opts.object_type or opts.object_type == "schema":
            objects.append(SchemaObject(type="schema", name="main"))

        # Search tables
        if not opts.object_type or opts.object_type == "table":
            cursor = await self._db.execute(
                """
                SELECT name
                FROM sqlite_master
                WHERE type = 'table'
                AND name NOT LIKE 'sqlite_%'
                AND name LIKE ?
                ORDER BY name
                """,
                (pattern,),
            )
            rows = await cursor.fetchall()
            for row in rows:
                objects.append(
                    SchemaObject(type="table", name=row["name"], schema="main")
                )

        # Search columns
        if opts.object_type == "column" and opts.table:
            # Use PRAGMA to get column info
            # Note: Need to sanitize table name to prevent SQL injection
            safe_table = opts.table.replace('"', '""')
            cursor = await self._db.execute(f'PRAGMA table_info("{safe_table}")')
            rows = await cursor.fetchall()

            for row in rows:
                col_name = row["name"]
                # Filter by pattern
                if pattern == "%" or pattern.lower().replace("%", "") in col_name.lower():
                    objects.append(
                        SchemaObject(
                            type="column",
                            name=col_name,
                            schema="main",
                            table=opts.table,
                            data_type=row["type"],
                            nullable=row["notnull"] == 0,
                            primary_key=row["pk"] == 1,
                        )
                    )

        # Search indexes
        if opts.object_type == "index":
            query = """
                SELECT name, tbl_name
                FROM sqlite_master
                WHERE type = 'index'
                AND name NOT LIKE 'sqlite_%'
                AND name LIKE ?
            """
            params: list[Any] = [pattern]

            if opts.table:
                query += " AND tbl_name = ?"
                params.append(opts.table)

            query += " ORDER BY tbl_name, name"

            cursor = await self._db.execute(query, params)
            rows = await cursor.fetchall()
            for row in rows:
                objects.append(
                    SchemaObject(
                        type="index",
                        name=row["name"],
                        schema="main",
                        table=row["tbl_name"],
                    )
                )

        return objects

    async def test_connection(self) -> bool:
        """Test the database connection."""
        try:
            await self.execute("SELECT 1")
            return True
        except Exception:
            return False

"""
MySQL/MariaDB connector using aiomysql.
"""

from typing import Any, Optional
from urllib.parse import urlparse

from ..config.types import SourceConfig
from ..constants import DEFAULT_PORTS
from ..utils.errors import ConnectionError, QueryError
from ..utils.formatters import QueryResult, SchemaObject
from ..utils.logger import logger
from .base import BaseConnector, ConnectorOptions, SchemaSearchOptions

try:
    import aiomysql
except ImportError:
    aiomysql = None  # type: ignore


class MySqlConnector(BaseConnector):
    """MySQL/MariaDB database connector using aiomysql."""

    def __init__(self, config: SourceConfig, options: Optional[ConnectorOptions] = None):
        super().__init__(config, options)
        self._pool: Optional[aiomysql.Pool] = None
        self._is_mariadb = config.type == "mariadb"

    @property
    def db_type(self) -> str:
        return "mariadb" if self._is_mariadb else "mysql"

    async def connect(self) -> None:
        if self._is_connected:
            return

        if aiomysql is None:
            raise ImportError(
                "aiomysql is required for MySQL/MariaDB. Install with: pip install aiomysql"
            )

        try:
            connection_kwargs = self._get_connection_kwargs()
            self._pool = await aiomysql.create_pool(
                **connection_kwargs,
                minsize=1,
                maxsize=5,
                connect_timeout=self._options.connection_timeout or 10,
            )

            # Test the connection
            async with self._pool.acquire() as conn:
                async with conn.cursor() as cur:
                    await cur.execute("SELECT 1")

            self._is_connected = True
            logger.info(f"Connected to {self.db_type}: {self.source_id}")

        except Exception as e:
            raise ConnectionError(self.source_id, e) from e

    async def disconnect(self) -> None:
        if self._pool:
            self._pool.close()
            await self._pool.wait_closed()
            self._pool = None
            self._is_connected = False
            logger.info(f"Disconnected from {self.db_type}: {self.source_id}")

    async def _execute_query(
        self,
        sql: str,
        params: Optional[list[Any]],
        max_rows: int,
        timeout: Optional[int],
    ) -> QueryResult:
        if not self._pool:
            raise QueryError(self.source_id, sql, Exception("Not connected"))

        try:
            wrapped_sql = self._wrap_with_limit(sql, max_rows + 1)

            async with self._pool.acquire() as conn:
                async with conn.cursor(aiomysql.DictCursor) as cur:
                    if params:
                        await cur.execute(wrapped_sql, params)
                    else:
                        await cur.execute(wrapped_sql)

                    # Check if it's a SELECT query
                    if cur.description:
                        rows = await cur.fetchall()
                        columns = [desc[0] for desc in cur.description]

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
                        return QueryResult(
                            columns=["affectedRows", "insertId"],
                            rows=[{"affectedRows": cur.rowcount, "insertId": cur.lastrowid}],
                            row_count=cur.rowcount,
                            truncated=False,
                        )

        except Exception as e:
            raise QueryError(self.source_id, sql, e) from e

    async def search_objects(
        self, options: Optional[SchemaSearchOptions] = None
    ) -> list[SchemaObject]:
        if not self._pool:
            raise Exception("Not connected")

        opts = options or SchemaSearchOptions()
        objects: list[SchemaObject] = []
        pattern = f"%{opts.pattern}%" if opts.pattern else "%"

        async with self._pool.acquire() as conn:
            async with conn.cursor(aiomysql.DictCursor) as cur:
                # Search schemas (databases in MySQL)
                if not opts.object_type or opts.object_type == "schema":
                    await cur.execute(
                        """
                        SELECT SCHEMA_NAME
                        FROM information_schema.SCHEMATA
                        WHERE SCHEMA_NAME NOT IN ('mysql', 'information_schema', 'performance_schema', 'sys')
                        AND SCHEMA_NAME LIKE %s
                        ORDER BY SCHEMA_NAME
                        """,
                        (pattern,),
                    )
                    rows = await cur.fetchall()
                    for row in rows:
                        objects.append(SchemaObject(type="schema", name=row["SCHEMA_NAME"]))

                # Search tables
                if not opts.object_type or opts.object_type == "table":
                    query = """
                        SELECT TABLE_SCHEMA, TABLE_NAME
                        FROM information_schema.TABLES
                        WHERE TABLE_SCHEMA NOT IN ('mysql', 'information_schema', 'performance_schema', 'sys')
                        AND TABLE_TYPE = 'BASE TABLE'
                        AND TABLE_NAME LIKE %s
                    """
                    params: list[Any] = [pattern]

                    if opts.schema:
                        query += " AND TABLE_SCHEMA = %s"
                        params.append(opts.schema)

                    query += " ORDER BY TABLE_SCHEMA, TABLE_NAME LIMIT 100"

                    await cur.execute(query, params)
                    rows = await cur.fetchall()
                    for row in rows:
                        objects.append(
                            SchemaObject(
                                type="table",
                                name=row["TABLE_NAME"],
                                schema=row["TABLE_SCHEMA"],
                            )
                        )

                # Search columns
                if opts.object_type == "column" and opts.table:
                    query = """
                        SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, COLUMN_KEY
                        FROM information_schema.COLUMNS
                        WHERE TABLE_NAME = %s
                        AND COLUMN_NAME LIKE %s
                    """
                    params = [opts.table, pattern]

                    if opts.schema:
                        query += " AND TABLE_SCHEMA = %s"
                        params.append(opts.schema)

                    query += " ORDER BY ORDINAL_POSITION"

                    await cur.execute(query, params)
                    rows = await cur.fetchall()
                    for row in rows:
                        objects.append(
                            SchemaObject(
                                type="column",
                                name=row["COLUMN_NAME"],
                                schema=opts.schema,
                                table=opts.table,
                                data_type=row["DATA_TYPE"],
                                nullable=row["IS_NULLABLE"] == "YES",
                                primary_key=row["COLUMN_KEY"] == "PRI",
                            )
                        )

                # Search indexes
                if opts.object_type == "index":
                    query = """
                        SELECT TABLE_SCHEMA, TABLE_NAME, INDEX_NAME
                        FROM information_schema.STATISTICS
                        WHERE TABLE_SCHEMA NOT IN ('mysql', 'information_schema', 'performance_schema', 'sys')
                        AND INDEX_NAME LIKE %s
                    """
                    params = [pattern]

                    if opts.schema:
                        query += " AND TABLE_SCHEMA = %s"
                        params.append(opts.schema)

                    if opts.table:
                        query += " AND TABLE_NAME = %s"
                        params.append(opts.table)

                    query += " GROUP BY TABLE_SCHEMA, TABLE_NAME, INDEX_NAME"
                    query += " ORDER BY TABLE_SCHEMA, TABLE_NAME, INDEX_NAME LIMIT 100"

                    await cur.execute(query, params)
                    rows = await cur.fetchall()
                    for row in rows:
                        objects.append(
                            SchemaObject(
                                type="index",
                                name=row["INDEX_NAME"],
                                schema=row["TABLE_SCHEMA"],
                                table=row["TABLE_NAME"],
                            )
                        )

                # Search procedures
                if opts.object_type == "procedure":
                    query = """
                        SELECT ROUTINE_SCHEMA, ROUTINE_NAME
                        FROM information_schema.ROUTINES
                        WHERE ROUTINE_SCHEMA NOT IN ('mysql', 'information_schema', 'performance_schema', 'sys')
                        AND ROUTINE_NAME LIKE %s
                    """
                    params = [pattern]

                    if opts.schema:
                        query += " AND ROUTINE_SCHEMA = %s"
                        params.append(opts.schema)

                    query += " ORDER BY ROUTINE_SCHEMA, ROUTINE_NAME LIMIT 100"

                    await cur.execute(query, params)
                    rows = await cur.fetchall()
                    for row in rows:
                        objects.append(
                            SchemaObject(
                                type="procedure",
                                name=row["ROUTINE_NAME"],
                                schema=row["ROUTINE_SCHEMA"],
                            )
                        )

        return objects

    def _get_connection_kwargs(self) -> dict[str, Any]:
        """Get connection kwargs from config."""
        config = self._config

        if hasattr(config, "dsn") and config.dsn:  # type: ignore
            # Parse DSN
            dsn = config.dsn  # type: ignore
            parsed = urlparse(dsn)

            kwargs: dict[str, Any] = {
                "host": parsed.hostname or "localhost",
                "port": parsed.port or DEFAULT_PORTS["mysql"],
            }
            if parsed.username:
                kwargs["user"] = parsed.username
            if parsed.password:
                kwargs["password"] = parsed.password
            if parsed.path and parsed.path != "/":
                kwargs["db"] = parsed.path.lstrip("/")
            return kwargs

        if hasattr(config, "host"):
            host_config = config  # type: ignore
            kwargs = {
                "host": host_config.host,
                "port": host_config.port or DEFAULT_PORTS["mysql"],
            }
            if hasattr(host_config, "database") and host_config.database:
                kwargs["db"] = host_config.database
            if hasattr(host_config, "user") and host_config.user:
                kwargs["user"] = host_config.user
            if hasattr(host_config, "password") and host_config.password:
                kwargs["password"] = host_config.password
            # aiomysql doesn't support ssl in the same way, would need ssl context
            return kwargs

        raise ValueError("Invalid MySQL/MariaDB configuration")

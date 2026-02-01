"""
PostgreSQL connector using asyncpg.
"""

from typing import Any, Optional

from ..config.types import DsnSourceConfig, HostBasedSourceConfig, SourceConfig
from ..constants import DEFAULT_PORTS
from ..utils.errors import ConnectionError, QueryError
from ..utils.formatters import QueryResult, SchemaObject
from ..utils.logger import logger
from .base import BaseConnector, ConnectorOptions, SchemaSearchOptions

try:
    import asyncpg
except ImportError:
    asyncpg = None  # type: ignore


class PostgresConnector(BaseConnector):
    """PostgreSQL database connector using asyncpg."""

    def __init__(self, config: SourceConfig, options: Optional[ConnectorOptions] = None):
        super().__init__(config, options)
        self._pool: Optional[asyncpg.Pool] = None

    @property
    def db_type(self) -> str:
        return "postgres"

    async def connect(self) -> None:
        if self._is_connected:
            return

        if asyncpg is None:
            raise ImportError("asyncpg is required for PostgreSQL. Install with: pip install asyncpg")

        try:
            connection_kwargs = self._get_connection_kwargs()
            self._pool = await asyncpg.create_pool(
                **connection_kwargs,
                min_size=1,
                max_size=5,
                timeout=self._options.connection_timeout,
            )

            # Test the connection
            async with self._pool.acquire() as conn:
                await conn.execute("SELECT 1")

            self._is_connected = True
            logger.info(f"Connected to PostgreSQL: {self.source_id}")

        except Exception as e:
            raise ConnectionError(self.source_id, e) from e

    async def disconnect(self) -> None:
        if self._pool:
            await self._pool.close()
            self._pool = None
            self._is_connected = False
            logger.info(f"Disconnected from PostgreSQL: {self.source_id}")

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
                # asyncpg uses $1, $2, etc. for parameters
                if params:
                    rows = await conn.fetch(wrapped_sql, *params, timeout=timeout)
                else:
                    rows = await conn.fetch(wrapped_sql, timeout=timeout)

            if not rows:
                return QueryResult(columns=[], rows=[], row_count=0, truncated=False)

            # Get column names from first row
            columns = list(rows[0].keys())

            # Convert rows to dicts
            row_dicts = [dict(row) for row in rows]
            formatted_rows, truncated = self._format_rows(row_dicts, max_rows)

            return QueryResult(
                columns=columns,
                rows=formatted_rows,
                row_count=len(rows),
                truncated=truncated,
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
            # Search schemas
            if not opts.object_type or opts.object_type == "schema":
                rows = await conn.fetch(
                    """
                    SELECT schema_name
                    FROM information_schema.schemata
                    WHERE schema_name NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
                    AND schema_name LIKE $1
                    ORDER BY schema_name
                    """,
                    pattern,
                )
                for row in rows:
                    objects.append(SchemaObject(type="schema", name=row["schema_name"]))

            # Search tables
            if not opts.object_type or opts.object_type == "table":
                query = """
                    SELECT table_schema, table_name
                    FROM information_schema.tables
                    WHERE table_schema NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
                    AND table_type = 'BASE TABLE'
                    AND table_name LIKE $1
                """
                params: list[Any] = [pattern]

                if opts.schema:
                    query += " AND table_schema = $2"
                    params.append(opts.schema)

                query += " ORDER BY table_schema, table_name LIMIT 100"

                rows = await conn.fetch(query, *params)
                for row in rows:
                    objects.append(
                        SchemaObject(
                            type="table",
                            name=row["table_name"],
                            schema=row["table_schema"],
                        )
                    )

            # Search columns
            if opts.object_type == "column" and opts.table:
                query = """
                    SELECT c.column_name, c.data_type, c.is_nullable,
                           CASE WHEN pk.column_name IS NOT NULL THEN true ELSE false END as is_primary_key
                    FROM information_schema.columns c
                    LEFT JOIN (
                        SELECT kcu.column_name, kcu.table_name, kcu.table_schema
                        FROM information_schema.table_constraints tc
                        JOIN information_schema.key_column_usage kcu
                            ON tc.constraint_name = kcu.constraint_name
                            AND tc.table_schema = kcu.table_schema
                        WHERE tc.constraint_type = 'PRIMARY KEY'
                    ) pk ON c.column_name = pk.column_name
                        AND c.table_name = pk.table_name
                        AND c.table_schema = pk.table_schema
                    WHERE c.table_name = $1
                    AND c.column_name LIKE $2
                """
                params = [opts.table, pattern]

                if opts.schema:
                    query += " AND c.table_schema = $3"
                    params.append(opts.schema)

                query += " ORDER BY c.ordinal_position"

                rows = await conn.fetch(query, *params)
                for row in rows:
                    objects.append(
                        SchemaObject(
                            type="column",
                            name=row["column_name"],
                            schema=opts.schema,
                            table=opts.table,
                            data_type=row["data_type"],
                            nullable=row["is_nullable"] == "YES",
                            primary_key=row["is_primary_key"],
                        )
                    )

            # Search indexes
            if opts.object_type == "index":
                query = """
                    SELECT schemaname, tablename, indexname
                    FROM pg_indexes
                    WHERE schemaname NOT IN ('pg_catalog', 'information_schema')
                    AND indexname LIKE $1
                """
                params = [pattern]

                if opts.schema:
                    query += " AND schemaname = $2"
                    params.append(opts.schema)

                if opts.table:
                    param_num = len(params) + 1
                    query += f" AND tablename = ${param_num}"
                    params.append(opts.table)

                query += " ORDER BY schemaname, tablename, indexname LIMIT 100"

                rows = await conn.fetch(query, *params)
                for row in rows:
                    objects.append(
                        SchemaObject(
                            type="index",
                            name=row["indexname"],
                            schema=row["schemaname"],
                            table=row["tablename"],
                        )
                    )

            # Search procedures/functions
            if opts.object_type == "procedure":
                query = """
                    SELECT n.nspname as schema_name, p.proname as proc_name
                    FROM pg_proc p
                    JOIN pg_namespace n ON p.pronamespace = n.oid
                    WHERE n.nspname NOT IN ('pg_catalog', 'information_schema')
                    AND p.proname LIKE $1
                """
                params = [pattern]

                if opts.schema:
                    query += " AND n.nspname = $2"
                    params.append(opts.schema)

                query += " ORDER BY n.nspname, p.proname LIMIT 100"

                rows = await conn.fetch(query, *params)
                for row in rows:
                    objects.append(
                        SchemaObject(
                            type="procedure",
                            name=row["proc_name"],
                            schema=row["schema_name"],
                        )
                    )

        return objects

    def _get_connection_kwargs(self) -> dict[str, Any]:
        """Get connection kwargs from config."""
        config = self._config

        if hasattr(config, "dsn") and config.dsn:  # type: ignore
            return {"dsn": config.dsn}  # type: ignore

        if hasattr(config, "host"):
            host_config = config  # type: ignore
            kwargs: dict[str, Any] = {
                "host": host_config.host,
                "port": host_config.port or DEFAULT_PORTS["postgres"],
            }
            if hasattr(host_config, "database") and host_config.database:
                kwargs["database"] = host_config.database
            if hasattr(host_config, "user") and host_config.user:
                kwargs["user"] = host_config.user
            if hasattr(host_config, "password") and host_config.password:
                kwargs["password"] = host_config.password
            if hasattr(host_config, "ssl") and host_config.ssl:
                kwargs["ssl"] = "require"
            return kwargs

        raise ValueError("Invalid PostgreSQL configuration")

"""
Apache Hive connector using pyhive with Kerberos support.
"""

import asyncio
from typing import Any, Optional

from ..config.types import KerberosSourceConfig, SourceConfig
from ..constants import DEFAULT_PORTS
from ..services.kerberos import KerberosAuth, KerberosConfig
from ..utils.errors import ConnectionError, QueryError
from ..utils.formatters import QueryResult, SchemaObject
from ..utils.logger import logger
from .base import BaseConnector, ConnectorOptions, SchemaSearchOptions

# pyhive is synchronous, we'll wrap with asyncio.to_thread
try:
    from pyhive import hive
    from thrift.transport.TTransport import TTransportException
except ImportError:
    hive = None  # type: ignore
    TTransportException = Exception  # type: ignore


class HiveConnector(BaseConnector):
    """Apache Hive database connector using pyhive."""

    def __init__(self, config: SourceConfig, options: Optional[ConnectorOptions] = None):
        super().__init__(config, options)
        self._connection: Any = None
        self._kerberos_auth: Optional[KerberosAuth] = None

    @property
    def db_type(self) -> str:
        return "hive"

    async def connect(self) -> None:
        if self._is_connected:
            return

        if hive is None:
            raise ImportError(
                "pyhive is required for Hive. Install with: pip install 'pyhive[hive]'"
            )

        config = self._config
        if not isinstance(config, KerberosSourceConfig):
            # Try to adapt non-Kerberos config
            if not hasattr(config, "host"):
                raise ValueError("Invalid Hive configuration: host is required")

        try:
            # Initialize Kerberos if needed
            if isinstance(config, KerberosSourceConfig):
                if (
                    config.auth_mechanism == "KERBEROS"
                    and config.keytab
                    and config.user_principal
                ):
                    self._kerberos_auth = KerberosAuth(
                        KerberosConfig(keytab=config.keytab, principal=config.user_principal)
                    )
                    await self._kerberos_auth.initialize()

            # Connect in a thread pool since pyhive is synchronous
            self._connection = await asyncio.to_thread(self._create_connection)

            self._is_connected = True
            logger.info(f"Connected to Hive: {self.source_id}")

        except Exception as e:
            if self._kerberos_auth:
                await self._kerberos_auth.destroy()
            raise ConnectionError(self.source_id, e) from e

    def _create_connection(self) -> Any:
        """Create Hive connection (synchronous, called via to_thread)."""
        config = self._config

        host = getattr(config, "host", "localhost")
        port = getattr(config, "port", None) or DEFAULT_PORTS["hive"]
        database = getattr(config, "database", "default")

        # Determine auth mechanism
        auth: Optional[str] = None
        kerberos_service_name: Optional[str] = None

        if isinstance(config, KerberosSourceConfig):
            if config.auth_mechanism == "KERBEROS":
                auth = "KERBEROS"
                kerberos_service_name = config.principal or "hive"
            elif config.auth_mechanism == "PLAIN":
                auth = "LDAP"
            else:
                auth = "NONE"
        else:
            auth = "NONE"

        return hive.connect(
            host=host,
            port=port,
            database=database,
            auth=auth,
            kerberos_service_name=kerberos_service_name,
        )

    async def disconnect(self) -> None:
        try:
            if self._connection:
                await asyncio.to_thread(self._connection.close)
            if self._kerberos_auth:
                await self._kerberos_auth.destroy()
        except Exception as e:
            logger.warning(f"Error during Hive disconnect: {e}")
        finally:
            self._connection = None
            self._kerberos_auth = None
            self._is_connected = False
            logger.info(f"Disconnected from Hive: {self.source_id}")

    async def _execute_query(
        self,
        sql: str,
        params: Optional[list[Any]],
        max_rows: int,
        timeout: Optional[int],
    ) -> QueryResult:
        if not self._connection:
            raise QueryError(self.source_id, sql, Exception("Not connected"))

        try:
            # Execute in thread pool
            result = await asyncio.to_thread(
                self._execute_sync, sql, params, max_rows
            )
            return result

        except Exception as e:
            raise QueryError(self.source_id, sql, e) from e

    def _execute_sync(
        self, sql: str, params: Optional[list[Any]], max_rows: int
    ) -> QueryResult:
        """Execute query synchronously (called via to_thread)."""
        cursor = self._connection.cursor()
        try:
            if params:
                cursor.execute(sql, params)
            else:
                cursor.execute(sql)

            # Fetch results
            if cursor.description:
                columns = [desc[0] for desc in cursor.description]

                # Fetch max_rows + 1 to detect truncation
                rows = []
                for i, row in enumerate(cursor):
                    if i > max_rows:
                        break
                    rows.append(dict(zip(columns, row)))

                formatted_rows, truncated = self._format_rows_sync(rows, max_rows)

                return QueryResult(
                    columns=columns,
                    rows=formatted_rows,
                    row_count=len(rows),
                    truncated=truncated,
                )
            else:
                return QueryResult(
                    columns=[],
                    rows=[],
                    row_count=0,
                    truncated=False,
                )
        finally:
            cursor.close()

    def _format_rows_sync(
        self, rows: list[dict[str, Any]], max_rows: int
    ) -> tuple[list[dict[str, Any]], bool]:
        """Format result rows and check for truncation."""
        truncated = len(rows) > max_rows
        limited_rows = rows[:max_rows] if truncated else rows
        return limited_rows, truncated

    async def search_objects(
        self, options: Optional[SchemaSearchOptions] = None
    ) -> list[SchemaObject]:
        opts = options or SchemaSearchOptions()
        objects: list[SchemaObject] = []
        pattern = opts.pattern or "%"

        # Search schemas (databases in Hive)
        if not opts.object_type or opts.object_type == "schema":
            result = await self.execute("SHOW DATABASES")
            for row in result.rows:
                db_name = list(row.values())[0]
                if isinstance(db_name, str):
                    if pattern == "%" or pattern.lower().replace("%", "") in db_name.lower():
                        objects.append(SchemaObject(type="schema", name=db_name))

        # Search tables
        if not opts.object_type or opts.object_type == "table":
            database = opts.schema or "default"
            result = await self.execute(f"SHOW TABLES IN {database}")

            for row in result.rows:
                table_name = list(row.values())[0]
                if isinstance(table_name, str):
                    if pattern == "%" or pattern.lower().replace("%", "") in table_name.lower():
                        objects.append(
                            SchemaObject(type="table", name=table_name, schema=database)
                        )

        # Search columns
        if opts.object_type == "column" and opts.table:
            database = opts.schema or "default"
            result = await self.execute(f"DESCRIBE {database}.{opts.table}")

            for row in result.rows:
                col_name = row.get("col_name") or row.get("column_name") or list(row.values())[0]
                data_type = row.get("data_type") or (list(row.values())[1] if len(row) > 1 else None)

                if col_name and isinstance(col_name, str) and not col_name.startswith("#"):
                    if pattern == "%" or pattern.lower().replace("%", "") in col_name.lower():
                        objects.append(
                            SchemaObject(
                                type="column",
                                name=col_name,
                                schema=database,
                                table=opts.table,
                                data_type=str(data_type) if data_type else None,
                                nullable=True,
                                primary_key=False,
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

"""
Database connectors module for OpenDB MCP Server.
"""

from typing import Any

from ..config.types import ParsedConfig, SourceConfig
from ..utils.formatters import SourceInfo
from ..utils.logger import logger
from .base import BaseConnector, ConnectorOptions, ExecuteOptions, SchemaSearchOptions
from .hive import HiveConnector
from .impala import ImpalaConnector
from .mysql import MySqlConnector
from .postgres import PostgresConnector
from .sqlite import SqliteConnector


class ConnectorManager:
    """Multi-database registry and factory."""

    def __init__(self, config: ParsedConfig):
        self._connectors: dict[str, BaseConnector] = {}
        self._global_options = ConnectorOptions(
            readonly=config.settings.readonly,
            max_rows=config.settings.max_rows,
            query_timeout=config.settings.query_timeout,
            connection_timeout=config.settings.connection_timeout,
        )

        # Create connectors for all sources
        for source_id, source_config in config.sources.items():
            connector = self._create_connector(source_config)
            self._connectors[source_id] = connector

    def _create_connector(self, config: SourceConfig) -> BaseConnector:
        """Factory method to create appropriate connector by type."""
        # Source-level readonly overrides global
        readonly = config.readonly if config.readonly is not None else self._global_options.readonly

        options = ConnectorOptions(
            readonly=readonly,
            max_rows=self._global_options.max_rows,
            query_timeout=self._global_options.query_timeout,
            connection_timeout=self._global_options.connection_timeout,
        )

        source_type = config.type

        if source_type == "postgres":
            return PostgresConnector(config, options)
        elif source_type in ("mysql", "mariadb"):
            return MySqlConnector(config, options)
        elif source_type == "sqlite":
            return SqliteConnector(config, options)
        elif source_type == "hive":
            return HiveConnector(config, options)
        elif source_type == "impala":
            return ImpalaConnector(config, options)
        else:
            raise ValueError(f"Unsupported database type: {source_type}")

    def get(self, source_id: str) -> BaseConnector | None:
        """Get a connector by source ID."""
        return self._connectors.get(source_id)

    def get_default(self) -> BaseConnector | None:
        """Get the default connector (first one if only one configured)."""
        if len(self._connectors) == 1:
            return next(iter(self._connectors.values()))
        return self._connectors.get("default")

    def resolve(self, source_id: str | None = None) -> BaseConnector:
        """Get a connector, resolving source ID or using default."""
        if source_id:
            connector = self.get(source_id)
            if not connector:
                available = ", ".join(self.list_source_ids())
                raise ValueError(f"Unknown source: {source_id}. Available sources: {available}")
            return connector

        default_connector = self.get_default()
        if not default_connector:
            available = ", ".join(self.list_source_ids())
            raise ValueError(
                f"Multiple sources configured. Please specify source_id. Available: {available}"
            )
        return default_connector

    def list_source_ids(self) -> list[str]:
        """List all source IDs."""
        return list(self._connectors.keys())

    def list_sources(self) -> list[SourceInfo]:
        """List all sources with their types."""
        return [
            SourceInfo(
                id=source_id,
                type=connector.db_type,
                readonly=connector.options.readonly,
                connected=connector.is_connected,
            )
            for source_id, connector in self._connectors.items()
        ]

    async def connect(self, source_id: str) -> None:
        """Connect to a specific source."""
        connector = self.get(source_id)
        if not connector:
            raise ValueError(f"Unknown source: {source_id}")
        await connector.connect()

    async def connect_all(self) -> None:
        """Connect to all sources."""
        errors: list[tuple[str, Exception]] = []

        for source_id, connector in self._connectors.items():
            try:
                await connector.connect()
            except Exception as e:
                errors.append((source_id, e))
                logger.error(f"Failed to connect to {source_id}: {e}")

        if errors and len(errors) == len(self._connectors):
            error_msgs = "; ".join(f"{sid}: {e}" for sid, e in errors)
            raise RuntimeError(f"Failed to connect to all sources: {error_msgs}")

    async def disconnect_all(self) -> None:
        """Disconnect from all sources."""
        for source_id, connector in self._connectors.items():
            try:
                await connector.disconnect()
            except Exception as e:
                logger.error(f"Failed to disconnect from {source_id}: {e}")

    @property
    def size(self) -> int:
        """Get the number of configured sources."""
        return len(self._connectors)


__all__ = [
    "ConnectorManager",
    "BaseConnector",
    "ConnectorOptions",
    "ExecuteOptions",
    "SchemaSearchOptions",
    "PostgresConnector",
    "MySqlConnector",
    "SqliteConnector",
    "HiveConnector",
    "ImpalaConnector",
]

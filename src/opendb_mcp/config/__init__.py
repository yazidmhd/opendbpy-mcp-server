"""
Configuration module for OpenDB MCP Server.
"""

from .keytab import KeytabError, process_keytab_contents
from .loader import create_config_from_dsn, load_config
from .types import (
    AuthMechanism,
    BaseSourceConfig,
    DatabaseType,
    DsnSourceConfig,
    HostBasedSourceConfig,
    KerberosSourceConfig,
    OpenDBConfig,
    ParsedConfig,
    Settings,
    SourceConfig,
)

__all__ = [
    "load_config",
    "create_config_from_dsn",
    "DatabaseType",
    "AuthMechanism",
    "BaseSourceConfig",
    "DsnSourceConfig",
    "HostBasedSourceConfig",
    "KerberosSourceConfig",
    "SourceConfig",
    "Settings",
    "OpenDBConfig",
    "ParsedConfig",
    "KeytabError",
    "process_keytab_contents",
]

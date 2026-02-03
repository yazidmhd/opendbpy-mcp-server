"""
Pydantic models for configuration validation.
"""

from typing import Literal, Optional, Union

from pydantic import BaseModel, Field, field_validator

DatabaseType = Literal["postgres", "mysql", "mariadb", "hive", "impala"]
AuthMechanism = Literal["NONE", "PLAIN", "KERBEROS"]
ResponseFormat = Literal["markdown", "json"]
ObjectType = Literal["schema", "table", "column", "index", "procedure"]


class BaseSourceConfig(BaseModel):
    """Base configuration for all database sources."""

    id: str = Field(..., description="Unique identifier for this source")
    type: DatabaseType = Field(..., description="Database type")
    readonly: Optional[bool] = Field(None, description="Force read-only mode for this source")


class DsnSourceConfig(BaseSourceConfig):
    """Configuration for DSN-based connections."""

    dsn: str = Field(..., description="Database connection string (DSN)")


class HostBasedSourceConfig(BaseSourceConfig):
    """Configuration for host-based connections."""

    host: str = Field(..., description="Database host")
    port: Optional[int] = Field(None, description="Database port")
    database: Optional[str] = Field(None, description="Database name")
    user: Optional[str] = Field(None, description="Username")
    password: Optional[str] = Field(None, description="Password")
    ssl: Optional[bool] = Field(None, description="Enable SSL")


class KerberosSourceConfig(BaseSourceConfig):
    """Configuration for Kerberos-authenticated connections (Hive/Impala)."""

    type: Literal["hive", "impala"]
    host: str = Field(..., description="Database host")
    port: Optional[int] = Field(None, description="Database port")
    database: Optional[str] = Field(None, description="Database name")
    auth_mechanism: AuthMechanism = Field("NONE", description="Authentication mechanism")
    principal: Optional[str] = Field(None, description="Kerberos service principal")
    keytab: Optional[str] = Field(None, description="Path to keytab file")
    keytab_content: Optional[str] = Field(None, description="Base64-encoded keytab content")
    user_principal: Optional[str] = Field(None, description="User principal for kinit")


# Union type for all source configurations
SourceConfig = Union[
    DsnSourceConfig,
    HostBasedSourceConfig,
    KerberosSourceConfig,
]


class Settings(BaseModel):
    """Global settings for the MCP server."""

    readonly: bool = Field(False, description="Default read-only mode")
    max_rows: int = Field(1000, description="Maximum rows to return", ge=1, le=100000)
    query_timeout: Optional[int] = Field(30, description="Query timeout in seconds", ge=1)
    connection_timeout: Optional[int] = Field(
        10, description="Connection timeout in seconds", ge=1
    )

    @field_validator("max_rows")
    @classmethod
    def validate_max_rows(cls, v: int) -> int:
        if v < 1:
            return 1
        if v > 100000:
            return 100000
        return v


class OpenDBConfig(BaseModel):
    """Raw configuration as loaded from TOML."""

    settings: Settings = Field(default_factory=Settings)
    sources: list[dict] = Field(default_factory=list)


class ParsedConfig(BaseModel):
    """Parsed and validated configuration."""

    model_config = {"arbitrary_types_allowed": True}

    settings: Settings
    sources: dict[str, SourceConfig] = Field(default_factory=dict)


def parse_source_config(data: dict) -> SourceConfig:
    """Parse a source configuration dictionary into the appropriate model."""
    source_type = data.get("type")

    if source_type in ("hive", "impala"):
        # Check if it's a Kerberos config
        if "auth_mechanism" in data or "keytab" in data or "keytab_content" in data or "principal" in data:
            return KerberosSourceConfig(**data)
        # Fall through to host-based if no Kerberos fields
        if "host" in data:
            return HostBasedSourceConfig(**data)

    if "dsn" in data:
        return DsnSourceConfig(**data)

    if "host" in data:
        return HostBasedSourceConfig(**data)

    raise ValueError(f"Invalid source configuration: {data}")

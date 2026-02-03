"""
TOML configuration loader with environment variable substitution.
"""

import os
import stat
import sys
from pathlib import Path
from typing import Any

from ..constants import ENV_VAR_PATTERN
from ..utils.logger import logger
from .keytab import process_keytab_contents
from .types import ParsedConfig, Settings, parse_source_config

# Use tomllib for Python 3.11+, tomli for earlier versions
if sys.version_info >= (3, 11):
    import tomllib
else:
    try:
        import tomli as tomllib
    except ImportError as err:
        raise ImportError(
            "tomli is required for Python < 3.11. Install with: pip install tomli"
        ) from err


def substitute_env_vars(value: str) -> str:
    """
    Substitute environment variables in a string.

    Format: ${VAR_NAME} or ${VAR_NAME:-default}
    """

    def replacer(match: Any) -> str:
        var_expression = match.group(1)

        if ":-" in var_expression:
            var_name, default_value = var_expression.split(":-", 1)
        else:
            var_name = var_expression
            default_value = None

        env_value = os.environ.get(var_name)

        if env_value is not None:
            return env_value

        if default_value is not None:
            return default_value

        logger.warning(f"Environment variable {var_name} is not set and has no default")
        return match.group(0)  # Return original if no substitution

    return ENV_VAR_PATTERN.sub(replacer, value)


def substitute_env_vars_in_object(obj: Any) -> Any:
    """Recursively substitute environment variables in an object."""
    if isinstance(obj, str):
        return substitute_env_vars(obj)

    if isinstance(obj, list):
        return [substitute_env_vars_in_object(item) for item in obj]

    if isinstance(obj, dict):
        return {key: substitute_env_vars_in_object(value) for key, value in obj.items()}

    return obj


def validate_keytab(keytab_path: str) -> None:
    """Validate keytab file exists and has appropriate permissions."""
    resolved_path = Path(keytab_path).resolve()

    if not resolved_path.exists():
        raise FileNotFoundError(f"Keytab file not found: {resolved_path}")

    # Check file permissions (should not be world-readable)
    mode = resolved_path.stat().st_mode
    if mode & stat.S_IROTH:  # World-readable
        logger.warning(
            f"Keytab file {resolved_path} is world-readable. Consider restricting permissions."
        )


def load_config(config_path: str) -> ParsedConfig:
    """Load and parse a TOML configuration file."""
    resolved_path = Path(config_path).resolve()

    if not resolved_path.exists():
        raise FileNotFoundError(f"Configuration file not found: {resolved_path}")

    with open(resolved_path, "rb") as f:
        try:
            parsed = tomllib.load(f)
        except Exception as e:
            raise ValueError(f"Failed to parse TOML: {e}") from e

    # Substitute environment variables
    substituted = substitute_env_vars_in_object(parsed)

    # Parse settings
    settings_data = substituted.get("settings", {})
    settings = Settings(**settings_data)

    # Parse sources
    sources_data = substituted.get("sources", [])

    # Process keytab_content fields (decode base64 and write keytab files)
    config_dir = resolved_path.parent
    sources_data = process_keytab_contents(sources_data, config_dir)
    sources: dict[str, Any] = {}

    for source_data in sources_data:
        source_id = source_data.get("id")
        if not source_id:
            raise ValueError("Source configuration missing 'id' field")

        if source_id in sources:
            raise ValueError(f"Duplicate source ID: {source_id}")

        # Validate keytab for Kerberos sources
        source_type = source_data.get("type")
        if source_type in ("hive", "impala"):
            auth_mechanism = source_data.get("auth_mechanism")
            keytab = source_data.get("keytab")
            if auth_mechanism == "KERBEROS" and keytab:
                validate_keytab(keytab)

        source = parse_source_config(source_data)
        sources[source_id] = source

    return ParsedConfig(settings=settings, sources=sources)


def create_config_from_dsn(dsn: str) -> ParsedConfig:
    """Create a configuration from a DSN string for single-database mode."""
    # Determine database type from DSN prefix
    dsn_lower = dsn.lower()

    if dsn_lower.startswith(("postgres://", "postgresql://")):
        db_type = "postgres"
    elif dsn_lower.startswith("mysql://"):
        db_type = "mysql"
    elif dsn_lower.startswith("mariadb://"):
        db_type = "mariadb"
    else:
        raise ValueError(
            "Unsupported DSN format. Expected postgres://, postgresql://, mysql://, or mariadb://"
        )

    source = parse_source_config({"id": "default", "type": db_type, "dsn": dsn})

    return ParsedConfig(
        settings=Settings(readonly=False, max_rows=1000),
        sources={"default": source},
    )

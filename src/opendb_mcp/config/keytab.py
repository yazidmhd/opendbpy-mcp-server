"""
Keytab processing for Kerberos authentication.

Handles decoding base64-encoded keytab content and writing keytab files.
"""

import base64
import stat
from pathlib import Path

from ..utils.errors import ConfigurationError
from ..utils.logger import logger


class KeytabError(ConfigurationError):
    """Error related to keytab processing."""

    pass


def decode_keytab_content(keytab_content: str, source_id: str) -> bytes:
    """
    Decode base64-encoded keytab content.

    Args:
        keytab_content: Base64-encoded keytab content
        source_id: Source identifier for error messages

    Returns:
        Decoded keytab bytes

    Raises:
        KeytabError: If decoding fails
    """
    try:
        return base64.b64decode(keytab_content)
    except Exception as e:
        raise KeytabError(f"Failed to decode base64 keytab content for source '{source_id}': {e}")


def write_keytab_file(keytab_bytes: bytes, keytab_dir: Path, source_id: str) -> Path:
    """
    Write keytab bytes to a file with secure permissions.

    Args:
        keytab_bytes: Decoded keytab content
        keytab_dir: Directory to write the keytab file
        source_id: Source identifier used for filename

    Returns:
        Path to the written keytab file

    Raises:
        KeytabError: If writing fails
    """
    try:
        # Create keytab directory if it doesn't exist
        keytab_dir.mkdir(parents=True, exist_ok=True)

        keytab_path = keytab_dir / f"{source_id}.keytab"

        # Write the keytab file
        keytab_path.write_bytes(keytab_bytes)

        # Set permissions to 755 (owner read/write/execute, group and others read/execute)
        keytab_path.chmod(stat.S_IRWXU | stat.S_IRGRP | stat.S_IXGRP | stat.S_IROTH | stat.S_IXOTH)

        logger.info(f"Wrote keytab file for source '{source_id}': {keytab_path}")
        return keytab_path

    except Exception as e:
        raise KeytabError(f"Failed to write keytab file for source '{source_id}': {e}")


def process_keytab_contents(sources_data: list[dict], config_dir: Path) -> list[dict]:
    """
    Process keytab_content fields in source configurations.

    For Hive/Impala sources with keytab_content, decodes the base64 content,
    writes it to a keytab file, and updates the source config with the file path.

    Args:
        sources_data: List of source configuration dictionaries
        config_dir: Directory containing the config file (keytabs written relative to this)

    Returns:
        Updated list of source configurations with keytab paths set

    Raises:
        KeytabError: If keytab processing fails
    """
    keytab_dir = config_dir / "keytabs"

    for source in sources_data:
        source_type = source.get("type")
        keytab_content = source.get("keytab_content")

        # Only process Hive/Impala sources with keytab_content
        if source_type in ("hive", "impala") and keytab_content:
            source_id = source.get("id", "unknown")

            # Decode and write the keytab
            keytab_bytes = decode_keytab_content(keytab_content, source_id)
            keytab_path = write_keytab_file(keytab_bytes, keytab_dir, source_id)

            # Update the source config with the file path
            # keytab_content takes precedence over keytab
            source["keytab"] = str(keytab_path)

    return sources_data

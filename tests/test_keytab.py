"""Tests for keytab processing module."""

import base64
import os
import stat
from pathlib import Path

import pytest

from opendb_mcp.config.keytab import (
    KeytabError,
    decode_keytab_content,
    process_keytab_contents,
    write_keytab_file,
)


class TestDecodeKeytabContent:
    """Tests for decode_keytab_content function."""

    def test_valid_base64(self):
        """Test decoding valid base64 content."""
        original = b"test keytab content"
        encoded = base64.b64encode(original).decode("utf-8")

        result = decode_keytab_content(encoded, "test-source")

        assert result == original

    def test_valid_base64_binary_data(self):
        """Test decoding base64-encoded binary data."""
        original = bytes(range(256))  # All byte values
        encoded = base64.b64encode(original).decode("utf-8")

        result = decode_keytab_content(encoded, "test-source")

        assert result == original

    def test_invalid_base64_raises_error(self):
        """Test that invalid base64 raises KeytabError."""
        invalid_content = "not-valid-base64!!!"

        with pytest.raises(KeytabError) as exc_info:
            decode_keytab_content(invalid_content, "test-source")

        assert "test-source" in str(exc_info.value)
        assert "decode" in str(exc_info.value).lower()

    def test_empty_content(self):
        """Test decoding empty base64 content."""
        encoded = base64.b64encode(b"").decode("utf-8")

        result = decode_keytab_content(encoded, "test-source")

        assert result == b""


class TestWriteKeytabFile:
    """Tests for write_keytab_file function."""

    def test_file_written_with_correct_name(self, tmp_path):
        """Test keytab file is written with correct filename."""
        keytab_dir = tmp_path / "keytabs"
        keytab_bytes = b"test keytab content"

        result = write_keytab_file(keytab_bytes, keytab_dir, "my-source")

        assert result == keytab_dir / "my-source.keytab"
        assert result.exists()
        assert result.read_bytes() == keytab_bytes

    def test_file_written_with_755_permissions(self, tmp_path):
        """Test keytab file has 755 permissions."""
        keytab_dir = tmp_path / "keytabs"
        keytab_bytes = b"test keytab content"

        result = write_keytab_file(keytab_bytes, keytab_dir, "test-source")

        mode = result.stat().st_mode
        # Check for 755 (owner rwx, group rx, others rx)
        assert mode & 0o777 == 0o755

    def test_directory_created_automatically(self, tmp_path):
        """Test keytab directory is created if it doesn't exist."""
        keytab_dir = tmp_path / "nested" / "keytabs"
        keytab_bytes = b"test keytab content"

        assert not keytab_dir.exists()

        result = write_keytab_file(keytab_bytes, keytab_dir, "test-source")

        assert keytab_dir.exists()
        assert result.exists()

    def test_overwrites_existing_file(self, tmp_path):
        """Test that existing keytab files are overwritten."""
        keytab_dir = tmp_path / "keytabs"
        keytab_dir.mkdir(parents=True)

        # Create existing file
        existing_path = keytab_dir / "test-source.keytab"
        existing_path.write_bytes(b"old content")

        # Write new content
        new_content = b"new keytab content"
        result = write_keytab_file(new_content, keytab_dir, "test-source")

        assert result.read_bytes() == new_content


class TestProcessKeytabContents:
    """Tests for process_keytab_contents function."""

    def test_processes_hive_source_with_keytab_content(self, tmp_path):
        """Test processing Hive source with keytab_content."""
        original = b"hive keytab data"
        encoded = base64.b64encode(original).decode("utf-8")

        sources_data = [
            {
                "id": "hive-source",
                "type": "hive",
                "host": "hive.example.com",
                "keytab_content": encoded,
            }
        ]

        result = process_keytab_contents(sources_data, tmp_path)

        assert result[0]["keytab"] == str(tmp_path / "keytabs" / "hive-source.keytab")
        assert Path(result[0]["keytab"]).read_bytes() == original

    def test_processes_impala_source_with_keytab_content(self, tmp_path):
        """Test processing Impala source with keytab_content."""
        original = b"impala keytab data"
        encoded = base64.b64encode(original).decode("utf-8")

        sources_data = [
            {
                "id": "impala-source",
                "type": "impala",
                "host": "impala.example.com",
                "keytab_content": encoded,
            }
        ]

        result = process_keytab_contents(sources_data, tmp_path)

        assert result[0]["keytab"] == str(tmp_path / "keytabs" / "impala-source.keytab")
        assert Path(result[0]["keytab"]).read_bytes() == original

    def test_ignores_non_hive_impala_sources(self, tmp_path):
        """Test that non-Hive/Impala sources are not processed."""
        encoded = base64.b64encode(b"data").decode("utf-8")

        sources_data = [
            {
                "id": "postgres-source",
                "type": "postgres",
                "host": "pg.example.com",
                "keytab_content": encoded,  # Should be ignored
            }
        ]

        result = process_keytab_contents(sources_data, tmp_path)

        # keytab should not be set
        assert "keytab" not in result[0] or result[0].get("keytab") is None

    def test_ignores_sources_without_keytab_content(self, tmp_path):
        """Test that sources without keytab_content are not modified."""
        sources_data = [
            {
                "id": "hive-source",
                "type": "hive",
                "host": "hive.example.com",
                "keytab": "/existing/keytab.keytab",
            }
        ]

        result = process_keytab_contents(sources_data, tmp_path)

        assert result[0]["keytab"] == "/existing/keytab.keytab"

    def test_keytab_content_takes_precedence_over_keytab(self, tmp_path):
        """Test that keytab_content takes precedence over existing keytab."""
        original = b"new keytab content"
        encoded = base64.b64encode(original).decode("utf-8")

        sources_data = [
            {
                "id": "hive-source",
                "type": "hive",
                "host": "hive.example.com",
                "keytab": "/old/keytab.keytab",
                "keytab_content": encoded,
            }
        ]

        result = process_keytab_contents(sources_data, tmp_path)

        # keytab should be updated to the new file path
        assert result[0]["keytab"] == str(tmp_path / "keytabs" / "hive-source.keytab")
        assert Path(result[0]["keytab"]).read_bytes() == original

    def test_preserves_keytab_when_no_keytab_content(self, tmp_path):
        """Test that existing keytab is preserved when no keytab_content."""
        sources_data = [
            {
                "id": "hive-source",
                "type": "hive",
                "host": "hive.example.com",
                "keytab": "/etc/security/keytabs/user.keytab",
            }
        ]

        result = process_keytab_contents(sources_data, tmp_path)

        assert result[0]["keytab"] == "/etc/security/keytabs/user.keytab"

    def test_processes_multiple_sources(self, tmp_path):
        """Test processing multiple sources at once."""
        hive_content = b"hive keytab"
        impala_content = b"impala keytab"

        sources_data = [
            {
                "id": "hive-source",
                "type": "hive",
                "host": "hive.example.com",
                "keytab_content": base64.b64encode(hive_content).decode("utf-8"),
            },
            {
                "id": "postgres-source",
                "type": "postgres",
                "host": "pg.example.com",
            },
            {
                "id": "impala-source",
                "type": "impala",
                "host": "impala.example.com",
                "keytab_content": base64.b64encode(impala_content).decode("utf-8"),
            },
        ]

        result = process_keytab_contents(sources_data, tmp_path)

        assert Path(result[0]["keytab"]).read_bytes() == hive_content
        assert "keytab" not in result[1]
        assert Path(result[2]["keytab"]).read_bytes() == impala_content

    def test_invalid_base64_raises_error(self, tmp_path):
        """Test that invalid base64 in keytab_content raises KeytabError."""
        sources_data = [
            {
                "id": "hive-source",
                "type": "hive",
                "host": "hive.example.com",
                "keytab_content": "invalid-base64!!!",
            }
        ]

        with pytest.raises(KeytabError) as exc_info:
            process_keytab_contents(sources_data, tmp_path)

        assert "hive-source" in str(exc_info.value)

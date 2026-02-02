"""Tests for database connectors."""

import pytest
from opendb_mcp.connectors.base import BaseConnector, ConnectorOptions
from opendb_mcp.config.types import parse_source_config


class TestBaseConnector:
    """Tests for base connector functionality."""

    def test_write_query_detection(self):
        """Test detection of write queries."""
        config = parse_source_config({"id": "test", "type": "postgres", "host": "localhost"})

        # Create a minimal concrete implementation for testing
        class TestConnector(BaseConnector):
            @property
            def db_type(self):
                return "test"

            async def connect(self):
                pass

            async def disconnect(self):
                pass

            async def _execute_query(self, sql, params, max_rows, timeout):
                pass

            async def search_objects(self, options=None):
                return []

        connector = TestConnector(config, ConnectorOptions(readonly=True))

        # Test various query types
        assert connector._is_write_query("SELECT * FROM users") is False
        assert connector._is_write_query("INSERT INTO users VALUES (1)") is True
        assert connector._is_write_query("UPDATE users SET name = 'x'") is True
        assert connector._is_write_query("DELETE FROM users") is True
        assert connector._is_write_query("DROP TABLE users") is True
        assert connector._is_write_query("CREATE TABLE t (id int)") is True
        assert connector._is_write_query("ALTER TABLE users ADD col") is True
        assert connector._is_write_query("TRUNCATE TABLE users") is True

        # Case insensitive
        assert connector._is_write_query("select * from users") is False
        assert connector._is_write_query("insert into users") is True

        # With leading whitespace
        assert connector._is_write_query("  SELECT * FROM users") is False

    def test_limit_wrapping(self):
        """Test LIMIT clause wrapping."""
        config = parse_source_config({"id": "test", "type": "postgres", "host": "localhost"})

        class TestConnector(BaseConnector):
            @property
            def db_type(self):
                return "test"

            async def connect(self):
                pass

            async def disconnect(self):
                pass

            async def _execute_query(self, sql, params, max_rows, timeout):
                pass

            async def search_objects(self, options=None):
                return []

        connector = TestConnector(config)

        # Should add LIMIT
        result = connector._wrap_with_limit("SELECT * FROM users", 100)
        assert result == "SELECT * FROM users LIMIT 100"

        # Should not double-add LIMIT
        result = connector._wrap_with_limit("SELECT * FROM users LIMIT 10", 100)
        assert result == "SELECT * FROM users LIMIT 10"

        # Should not add to non-SELECT
        result = connector._wrap_with_limit("INSERT INTO users VALUES (1)", 100)
        assert result == "INSERT INTO users VALUES (1)"

        # Should handle ORDER BY
        result = connector._wrap_with_limit("SELECT * FROM users ORDER BY id", 100)
        assert result == "SELECT * FROM users ORDER BY id LIMIT 100"



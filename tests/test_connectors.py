"""Tests for database connectors."""

import pytest
from opendb_mcp.connectors.base import BaseConnector, ConnectorOptions
from opendb_mcp.config.types import parse_source_config


class TestBaseConnector:
    """Tests for base connector functionality."""

    def test_write_query_detection(self):
        """Test detection of write queries."""
        config = parse_source_config({"id": "test", "type": "sqlite", "path": ":memory:"})

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
        config = parse_source_config({"id": "test", "type": "sqlite", "path": ":memory:"})

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


@pytest.mark.asyncio
class TestSqliteConnector:
    """Tests for SQLite connector."""

    async def test_connect_memory(self):
        """Test connecting to in-memory SQLite database."""
        from opendb_mcp.connectors.sqlite import SqliteConnector

        config = parse_source_config({"id": "test", "type": "sqlite", "path": ":memory:"})
        connector = SqliteConnector(config)

        await connector.connect()
        assert connector.is_connected is True

        await connector.disconnect()
        assert connector.is_connected is False

    async def test_execute_query(self):
        """Test executing queries."""
        from opendb_mcp.connectors.sqlite import SqliteConnector

        config = parse_source_config({"id": "test", "type": "sqlite", "path": ":memory:"})
        connector = SqliteConnector(config)

        await connector.connect()

        # Create table
        await connector.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)")

        # Insert data
        await connector.execute("INSERT INTO test (name) VALUES ('Alice')")
        await connector.execute("INSERT INTO test (name) VALUES ('Bob')")

        # Query data
        result = await connector.execute("SELECT * FROM test")
        assert result.columns == ["id", "name"]
        assert len(result.rows) == 2
        assert result.rows[0]["name"] == "Alice"
        assert result.rows[1]["name"] == "Bob"

        await connector.disconnect()

    async def test_readonly_enforcement(self):
        """Test read-only mode enforcement."""
        from opendb_mcp.connectors.sqlite import SqliteConnector
        from opendb_mcp.utils.errors import QueryError

        config = parse_source_config({"id": "test", "type": "sqlite", "path": ":memory:"})
        connector = SqliteConnector(config, ConnectorOptions(readonly=True))

        await connector.connect()

        # This should work
        await connector.execute("SELECT 1")

        # This should fail
        with pytest.raises(QueryError):
            await connector.execute("CREATE TABLE test (id INTEGER)")

        await connector.disconnect()

    async def test_search_objects(self):
        """Test schema object search."""
        from opendb_mcp.connectors.sqlite import SqliteConnector

        config = parse_source_config({"id": "test", "type": "sqlite", "path": ":memory:"})
        connector = SqliteConnector(config)

        await connector.connect()

        # Create test schema
        await connector.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
        await connector.execute("CREATE INDEX idx_name ON users(name)")

        # Search all objects
        objects = await connector.search_objects()
        types = {obj.type for obj in objects}
        assert "schema" in types
        assert "table" in types

        # Search tables only
        from opendb_mcp.connectors.base import SchemaSearchOptions

        objects = await connector.search_objects(SchemaSearchOptions(object_type="table"))
        assert all(obj.type == "table" for obj in objects)
        assert any(obj.name == "users" for obj in objects)

        # Search columns
        objects = await connector.search_objects(
            SchemaSearchOptions(object_type="column", table="users")
        )
        assert all(obj.type == "column" for obj in objects)
        column_names = {obj.name for obj in objects}
        assert "id" in column_names
        assert "name" in column_names

        await connector.disconnect()

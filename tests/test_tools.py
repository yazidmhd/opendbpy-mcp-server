"""Tests for MCP tools."""

import pytest
from opendb_mcp.config.types import ParsedConfig, Settings, parse_source_config
from opendb_mcp.connectors import ConnectorManager
from opendb_mcp.tools import (
    ExecuteSqlInput,
    ListSourcesInput,
    SearchObjectsInput,
    execute_sql,
    list_sources,
    search_objects,
)


@pytest.fixture
async def manager():
    """Create a connector manager with SQLite for testing."""
    config = ParsedConfig(
        settings=Settings(readonly=False, max_rows=100),
        sources={
            "test-db": parse_source_config({
                "id": "test-db",
                "type": "sqlite",
                "path": ":memory:"
            })
        }
    )
    mgr = ConnectorManager(config)
    await mgr.connect_all()

    # Setup test data
    connector = mgr.resolve()
    await connector.execute(
        "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT)"
    )
    await connector.execute(
        "INSERT INTO users (name, email) VALUES ('Alice', 'alice@example.com')"
    )
    await connector.execute(
        "INSERT INTO users (name, email) VALUES ('Bob', 'bob@example.com')"
    )

    yield mgr

    await mgr.disconnect_all()


@pytest.mark.asyncio
class TestExecuteSql:
    """Tests for execute_sql tool."""

    async def test_basic_query(self, manager):
        """Test basic SQL query execution."""
        result = await execute_sql(
            manager,
            ExecuteSqlInput(sql="SELECT * FROM users ORDER BY id")
        )
        assert result.is_error is False
        assert "Alice" in result.content[0]["text"]
        assert "Bob" in result.content[0]["text"]

    async def test_empty_sql(self, manager):
        """Test error on empty SQL."""
        result = await execute_sql(
            manager,
            ExecuteSqlInput(sql="")
        )
        assert result.is_error is True
        assert "required" in result.content[0]["text"].lower()

    async def test_json_format(self, manager):
        """Test JSON response format."""
        result = await execute_sql(
            manager,
            ExecuteSqlInput(sql="SELECT * FROM users WHERE id = 1", response_format="json")
        )
        assert result.is_error is False
        assert '"columns"' in result.content[0]["text"]
        assert '"rows"' in result.content[0]["text"]


@pytest.mark.asyncio
class TestSearchObjects:
    """Tests for search_objects tool."""

    async def test_search_all(self, manager):
        """Test searching all objects."""
        result = await search_objects(
            manager,
            SearchObjectsInput()
        )
        assert result.is_error is False
        assert "users" in result.content[0]["text"]

    async def test_search_columns(self, manager):
        """Test searching columns."""
        result = await search_objects(
            manager,
            SearchObjectsInput(object_type="column", table="users")
        )
        assert result.is_error is False
        assert "id" in result.content[0]["text"]
        assert "name" in result.content[0]["text"]
        assert "email" in result.content[0]["text"]

    async def test_column_without_table(self, manager):
        """Test error when searching columns without table."""
        result = await search_objects(
            manager,
            SearchObjectsInput(object_type="column")
        )
        assert result.is_error is True
        assert "table" in result.content[0]["text"].lower()


@pytest.mark.asyncio
class TestListSources:
    """Tests for list_sources tool."""

    async def test_list_sources(self, manager):
        """Test listing sources."""
        result = await list_sources(
            manager,
            ListSourcesInput()
        )
        assert result.is_error is False
        assert "test-db" in result.content[0]["text"]
        assert "sqlite" in result.content[0]["text"]

    async def test_json_format(self, manager):
        """Test JSON response format."""
        result = await list_sources(
            manager,
            ListSourcesInput(response_format="json")
        )
        assert result.is_error is False
        assert '"id"' in result.content[0]["text"]
        assert '"type"' in result.content[0]["text"]

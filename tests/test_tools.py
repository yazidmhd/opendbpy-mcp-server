"""Tests for MCP tools."""

from unittest.mock import AsyncMock, MagicMock
import pytest
from opendb_mcp.connectors.base import ConnectorOptions, QueryResult
from opendb_mcp.tools import (
    ExecuteSqlInput,
    ListSourcesInput,
    SearchObjectsInput,
    execute_sql,
    list_sources,
    search_objects,
)
from opendb_mcp.utils.formatters import SchemaObject, SourceInfo


@pytest.fixture
def mock_connector():
    """Create a mock connector for testing."""
    connector = MagicMock()
    connector.db_type = "postgres"
    connector.is_connected = True
    connector.options = ConnectorOptions(readonly=False, max_rows=100)
    connector.execute = AsyncMock(return_value=QueryResult(
        columns=["id", "name", "email"],
        rows=[
            {"id": 1, "name": "Alice", "email": "alice@example.com"},
            {"id": 2, "name": "Bob", "email": "bob@example.com"},
        ],
        row_count=2,
    ))
    connector.search_objects = AsyncMock(return_value=[
        SchemaObject(type="table", name="users", schema="public"),
        SchemaObject(type="column", name="id", schema="public", table="users", data_type="integer"),
        SchemaObject(type="column", name="name", schema="public", table="users", data_type="text"),
        SchemaObject(type="column", name="email", schema="public", table="users", data_type="text"),
    ])
    return connector


@pytest.fixture
def manager(mock_connector):
    """Create a mock connector manager for testing."""
    mgr = MagicMock()
    mgr.resolve = MagicMock(return_value=mock_connector)
    mgr.list_sources = MagicMock(return_value=[
        SourceInfo(id="test-db", type="postgres", readonly=False, connected=True)
    ])
    return mgr


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

    async def test_search_columns(self, manager, mock_connector):
        """Test searching columns."""
        mock_connector.search_objects = AsyncMock(return_value=[
            SchemaObject(type="column", name="id", schema="public", table="users", data_type="integer"),
            SchemaObject(type="column", name="name", schema="public", table="users", data_type="text"),
            SchemaObject(type="column", name="email", schema="public", table="users", data_type="text"),
        ])
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
        assert "postgres" in result.content[0]["text"]

    async def test_json_format(self, manager):
        """Test JSON response format."""
        result = await list_sources(
            manager,
            ListSourcesInput(response_format="json")
        )
        assert result.is_error is False
        assert '"id"' in result.content[0]["text"]
        assert '"type"' in result.content[0]["text"]

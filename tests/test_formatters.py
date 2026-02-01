"""Tests for formatters module."""

import json
import pytest
from opendb_mcp.utils.formatters import (
    QueryResult,
    SchemaObject,
    SourceInfo,
    format_query_results,
    format_schema_objects,
    format_sources_list,
)


class TestQueryResultFormatting:
    """Tests for query result formatting."""

    def test_markdown_table(self):
        result = QueryResult(
            columns=["id", "name"],
            rows=[
                {"id": 1, "name": "Alice"},
                {"id": 2, "name": "Bob"},
            ],
            row_count=2,
            truncated=False,
        )
        output = format_query_results(result, "markdown")
        assert "| id | name |" in output
        assert "| 1 | Alice |" in output
        assert "| 2 | Bob |" in output
        assert "_Showing 2 of 2 rows_" in output

    def test_json_output(self):
        result = QueryResult(
            columns=["id", "name"],
            rows=[{"id": 1, "name": "Test"}],
            row_count=1,
            truncated=False,
        )
        output = format_query_results(result, "json")
        data = json.loads(output)
        assert data["columns"] == ["id", "name"]
        assert len(data["rows"]) == 1
        assert data["rowCount"] == 1
        assert data["truncated"] is False

    def test_empty_results(self):
        result = QueryResult(columns=[], rows=[], row_count=0, truncated=False)
        output = format_query_results(result, "markdown")
        assert "_No results returned_" in output

    def test_truncation_indicator(self):
        result = QueryResult(
            columns=["id"],
            rows=[{"id": 1}],
            row_count=100,
            truncated=True,
        )
        output = format_query_results(result, "markdown")
        assert "_(results truncated)_" in output


class TestSchemaObjectFormatting:
    """Tests for schema object formatting."""

    def test_grouped_output(self):
        objects = [
            SchemaObject(type="schema", name="public"),
            SchemaObject(type="table", name="users", schema="public"),
            SchemaObject(type="table", name="orders", schema="public"),
        ]
        output = format_schema_objects(objects, "markdown")
        assert "## Schemas" in output
        assert "## Tables" in output
        assert "- public" in output
        assert "- public.users" in output

    def test_column_table(self):
        objects = [
            SchemaObject(
                type="column",
                name="id",
                table="users",
                data_type="integer",
                nullable=False,
                primary_key=True,
            ),
            SchemaObject(
                type="column",
                name="name",
                table="users",
                data_type="text",
                nullable=True,
                primary_key=False,
            ),
        ]
        output = format_schema_objects(objects, "markdown")
        assert "| Column | Type | Nullable | Primary Key |" in output
        assert "| id | integer | No | Yes |" in output
        assert "| name | text | Yes | No |" in output

    def test_empty_objects(self):
        output = format_schema_objects([], "markdown")
        assert "_No objects found_" in output


class TestSourcesListFormatting:
    """Tests for sources list formatting."""

    def test_table_output(self):
        sources = [
            SourceInfo(id="pg-main", type="postgres", readonly=True, connected=True),
            SourceInfo(id="mysql-dev", type="mysql", readonly=False, connected=False),
        ]
        output = format_sources_list(sources, "markdown")
        assert "| ID | Type | Mode | Connected |" in output
        assert "| pg-main | postgres | Read-only | Yes |" in output
        assert "| mysql-dev | mysql | Read/Write | No |" in output

    def test_json_output(self):
        sources = [
            SourceInfo(id="test", type="sqlite", readonly=False, connected=True),
        ]
        output = format_sources_list(sources, "json")
        data = json.loads(output)
        assert len(data) == 1
        assert data[0]["id"] == "test"
        assert data[0]["connected"] is True

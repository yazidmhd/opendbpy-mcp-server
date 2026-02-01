"""
Result formatters for Markdown and JSON output.
"""

import json
from dataclasses import dataclass, field
from typing import Any, Literal, Optional

from ..constants import CHARACTER_LIMIT

ResponseFormat = Literal["markdown", "json"]


@dataclass
class QueryResult:
    """Result from a database query."""

    columns: list[str]
    rows: list[dict[str, Any]]
    row_count: int
    truncated: bool = False


@dataclass
class SchemaObject:
    """Database schema object."""

    type: Literal["schema", "table", "column", "index", "procedure"]
    name: str
    schema: Optional[str] = None
    table: Optional[str] = None
    data_type: Optional[str] = None
    nullable: Optional[bool] = None
    primary_key: Optional[bool] = None
    extra: dict[str, Any] = field(default_factory=dict)


def _format_value(value: Any) -> str:
    """Format a single value for display."""
    if value is None:
        return "_null_"
    if isinstance(value, (dict, list)):
        return json.dumps(value)
    return str(value).replace("|", "\\|").replace("\n", " ")


def _format_results_as_markdown(result: QueryResult) -> str:
    """Format query results as a Markdown table."""
    if not result.rows:
        return "_No results returned_"

    columns = result.columns
    rows = result.rows

    # Build header
    lines = [
        "| " + " | ".join(columns) + " |",
        "| " + " | ".join("---" for _ in columns) + " |",
    ]

    # Build rows
    for row in rows:
        values = [_format_value(row.get(col)) for col in columns]
        lines.append("| " + " | ".join(values) + " |")

    # Add summary
    lines.append("")
    summary = f"_Showing {len(rows)} of {result.row_count} rows_"
    if result.truncated:
        summary += " _(results truncated)_"
    lines.append(summary)

    return "\n".join(lines)


def _format_results_as_json(result: QueryResult) -> str:
    """Format query results as JSON."""
    return json.dumps(
        {
            "columns": result.columns,
            "rows": result.rows,
            "rowCount": result.row_count,
            "truncated": result.truncated,
        },
        indent=2,
        default=str,
    )


def format_query_results(result: QueryResult, fmt: ResponseFormat = "markdown") -> str:
    """Format query results based on requested format."""
    if fmt == "json":
        formatted = _format_results_as_json(result)
    else:
        formatted = _format_results_as_markdown(result)

    if len(formatted) > CHARACTER_LIMIT:
        truncate_msg = (
            '\n\n{"_truncated": true, "_message": "Output exceeded character limit"}'
            if fmt == "json"
            else "\n\n_Output truncated due to character limit_"
        )
        return formatted[: CHARACTER_LIMIT - len(truncate_msg)] + truncate_msg

    return formatted


def _format_schema_as_markdown(objects: list[SchemaObject]) -> str:
    """Format schema objects as Markdown."""
    if not objects:
        return "_No objects found_"

    # Group by type
    grouped: dict[str, list[SchemaObject]] = {}
    for obj in objects:
        if obj.type not in grouped:
            grouped[obj.type] = []
        grouped[obj.type].append(obj)

    lines = []

    for obj_type, items in grouped.items():
        # Capitalize type name and pluralize
        type_name = obj_type.capitalize() + "s"
        lines.append(f"## {type_name}")
        lines.append("")

        if obj_type == "column":
            # Table format for columns
            lines.append("| Column | Type | Nullable | Primary Key |")
            lines.append("| --- | --- | --- | --- |")
            for item in items:
                nullable = "Yes" if item.nullable else "No"
                pk = "Yes" if item.primary_key else "No"
                lines.append(f"| {item.name} | {item.data_type or '-'} | {nullable} | {pk} |")
        else:
            # List format for other types
            for item in items:
                qualified_name = f"{item.schema}.{item.name}" if item.schema else item.name
                lines.append(f"- {qualified_name}")

        lines.append("")

    return "\n".join(lines).strip()


def _format_schema_as_json(objects: list[SchemaObject]) -> str:
    """Format schema objects as JSON."""
    return json.dumps(
        [
            {
                "type": obj.type,
                "name": obj.name,
                "schema": obj.schema,
                "table": obj.table,
                "dataType": obj.data_type,
                "nullable": obj.nullable,
                "primaryKey": obj.primary_key,
            }
            for obj in objects
        ],
        indent=2,
    )


def format_schema_objects(objects: list[SchemaObject], fmt: ResponseFormat = "markdown") -> str:
    """Format schema objects based on requested format."""
    if fmt == "json":
        formatted = _format_schema_as_json(objects)
    else:
        formatted = _format_schema_as_markdown(objects)

    if len(formatted) > CHARACTER_LIMIT:
        truncate_msg = (
            '\n\n{"_truncated": true}'
            if fmt == "json"
            else "\n\n_Output truncated due to character limit_"
        )
        return formatted[: CHARACTER_LIMIT - len(truncate_msg)] + truncate_msg

    return formatted


@dataclass
class SourceInfo:
    """Information about a database source."""

    id: str
    type: str
    readonly: bool
    connected: bool = False


def format_sources_list(sources: list[SourceInfo], fmt: ResponseFormat = "markdown") -> str:
    """Format a list of database sources."""
    if fmt == "json":
        return json.dumps(
            [
                {
                    "id": s.id,
                    "type": s.type,
                    "readonly": s.readonly,
                    "connected": s.connected,
                }
                for s in sources
            ],
            indent=2,
        )

    if not sources:
        return "_No database sources configured_"

    lines = [
        "## Configured Database Sources",
        "",
        "| ID | Type | Mode | Connected |",
        "| --- | --- | --- | --- |",
    ]

    for source in sources:
        mode = "Read-only" if source.readonly else "Read/Write"
        connected = "Yes" if source.connected else "No"
        lines.append(f"| {source.id} | {source.type} | {mode} | {connected} |")

    return "\n".join(lines)

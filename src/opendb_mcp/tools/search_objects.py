"""
search_objects tool implementation.
"""

from dataclasses import dataclass
from typing import Literal, Optional

from ..connectors import ConnectorManager, SchemaSearchOptions
from ..utils.errors import format_error_for_response
from ..utils.formatters import format_schema_objects
from ..utils.logger import logger


@dataclass
class SearchObjectsInput:
    """Input parameters for search_objects tool."""

    source_id: Optional[str] = None
    object_type: Optional[Literal["schema", "table", "column", "index", "procedure"]] = None
    schema: Optional[str] = None
    table: Optional[str] = None
    pattern: Optional[str] = None
    response_format: Literal["markdown", "json"] = "markdown"


@dataclass
class SearchObjectsResult:
    """Result from search_objects tool."""

    content: list[dict[str, str]]
    is_error: bool = False


async def search_objects(
    connector_manager: ConnectorManager,
    input_data: SearchObjectsInput,
) -> SearchObjectsResult:
    """Explore database schemas with progressive disclosure."""
    source_id = input_data.source_id
    object_type = input_data.object_type
    schema = input_data.schema
    table = input_data.table
    pattern = input_data.pattern
    response_format = input_data.response_format

    # Validate column search requires table
    if object_type == "column" and not table:
        return SearchObjectsResult(
            content=[
                {"type": "text", "text": "Error: Table name is required when searching for columns"}
            ],
            is_error=True,
        )

    try:
        # Resolve connector
        connector = connector_manager.resolve(source_id)

        # Ensure connected
        if not connector.is_connected:
            await connector.connect()

        logger.debug(
            f"Searching objects on {connector.source_id}",
            {
                "object_type": object_type,
                "schema": schema,
                "table": table,
                "pattern": pattern,
            },
        )

        # Search objects
        objects = await connector.search_objects(
            SchemaSearchOptions(
                object_type=object_type,
                schema=schema,
                table=table,
                pattern=pattern,
            )
        )

        # Format response
        formatted = format_schema_objects(objects, response_format)

        return SearchObjectsResult(
            content=[{"type": "text", "text": formatted}],
            is_error=False,
        )

    except Exception as e:
        logger.error("Object search failed", e)
        return SearchObjectsResult(
            content=[{"type": "text", "text": format_error_for_response(e)}],
            is_error=True,
        )

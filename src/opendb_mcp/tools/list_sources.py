"""
list_sources tool implementation.
"""

from dataclasses import dataclass
from typing import Literal

from ..connectors import ConnectorManager
from ..utils.errors import format_error_for_response
from ..utils.formatters import format_sources_list
from ..utils.logger import logger


@dataclass
class ListSourcesInput:
    """Input parameters for list_sources tool."""

    response_format: Literal["markdown", "json"] = "markdown"


@dataclass
class ListSourcesResult:
    """Result from list_sources tool."""

    content: list[dict[str, str]]
    is_error: bool = False


async def list_sources(
    connector_manager: ConnectorManager,
    input_data: ListSourcesInput,
) -> ListSourcesResult:
    """List all configured database connections with their types and status."""
    response_format = input_data.response_format

    try:
        logger.debug("Listing configured database sources")

        # Get all sources
        sources = connector_manager.list_sources()

        # Format response
        formatted = format_sources_list(sources, response_format)

        return ListSourcesResult(
            content=[{"type": "text", "text": formatted}],
            is_error=False,
        )

    except Exception as e:
        logger.error("Failed to list sources", e)
        return ListSourcesResult(
            content=[{"type": "text", "text": format_error_for_response(e)}],
            is_error=True,
        )

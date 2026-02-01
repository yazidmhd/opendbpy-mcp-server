"""
execute_sql tool implementation.
"""

from dataclasses import dataclass, field
from typing import Any, Literal, Optional

from ..connectors import ConnectorManager, ExecuteOptions
from ..utils.errors import format_error_for_response
from ..utils.formatters import format_query_results
from ..utils.logger import logger


@dataclass
class ExecuteSqlInput:
    """Input parameters for execute_sql tool."""

    sql: str
    source_id: Optional[str] = None
    params: Optional[list[Any]] = None
    response_format: Literal["markdown", "json"] = "markdown"


@dataclass
class ExecuteSqlResult:
    """Result from execute_sql tool."""

    content: list[dict[str, str]]
    is_error: bool = False


async def execute_sql(
    connector_manager: ConnectorManager,
    input_data: ExecuteSqlInput,
) -> ExecuteSqlResult:
    """Execute SQL queries against configured database sources."""
    sql = input_data.sql
    source_id = input_data.source_id
    params = input_data.params
    response_format = input_data.response_format

    if not sql or not sql.strip():
        return ExecuteSqlResult(
            content=[{"type": "text", "text": "Error: SQL query is required"}],
            is_error=True,
        )

    try:
        # Resolve connector
        connector = connector_manager.resolve(source_id)

        # Ensure connected
        if not connector.is_connected:
            await connector.connect()

        logger.debug(f"Executing SQL on {connector.source_id}", {"sql": sql[:100]})

        # Execute query
        result = await connector.execute(sql, ExecuteOptions(params=params))

        # Format response
        formatted = format_query_results(result, response_format)

        return ExecuteSqlResult(
            content=[{"type": "text", "text": formatted}],
            is_error=False,
        )

    except Exception as e:
        logger.error("SQL execution failed", e)
        return ExecuteSqlResult(
            content=[{"type": "text", "text": format_error_for_response(e)}],
            is_error=True,
        )

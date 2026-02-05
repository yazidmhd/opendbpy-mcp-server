"""
MCP Server setup and transport handling.

Supports both stdio and streamable HTTP transports.
"""

from dataclasses import dataclass
from typing import Any, Literal, Optional

from mcp.server import Server
from mcp.server.stdio import stdio_server
from mcp.types import TextContent, Tool

from .config.types import ParsedConfig
from .connectors import ConnectorManager
from .constants import SERVER_NAME, SERVER_VERSION
from .tools import (
    ExecuteSqlInput,
    ListSourcesInput,
    SearchObjectsInput,
    execute_sql,
    list_sources,
    search_objects,
)
from .utils.logger import logger


@dataclass
class ServerOptions:
    """Options for the MCP server."""

    config: ParsedConfig
    transport: Literal["stdio", "http"] = "stdio"
    port: int = 3000


class OpenDBServer:
    """OpenDB MCP Server."""

    def __init__(self, options: ServerOptions):
        self.options = options
        self.connector_manager = ConnectorManager(options.config)
        self.server = Server(SERVER_NAME)
        self._http_server: Any = None
        self._http_runner: Any = None
        self._setup_handlers()

    def _setup_handlers(self) -> None:
        """Set up MCP server handlers."""

        @self.server.list_tools()
        async def list_tools() -> list[Tool]:
            """Return list of available tools."""
            return [
                Tool(
                    name="execute_sql",
                    description="Execute SQL queries against configured database sources. Supports prepared statements with parameterized queries.",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "source_id": {
                                "type": "string",
                                "description": "Database source ID (optional if single db configured)",
                            },
                            "sql": {
                                "type": "string",
                                "description": "SQL query to execute",
                            },
                            "params": {
                                "type": "array",
                                "items": {},
                                "description": "Prepared statement parameters",
                            },
                            "response_format": {
                                "type": "string",
                                "enum": ["markdown", "json"],
                                "default": "markdown",
                                "description": "Output format",
                            },
                        },
                        "required": ["sql"],
                    },
                ),
                Tool(
                    name="search_objects",
                    description="Explore database schemas with progressive disclosure. Search for schemas, tables, columns, indexes, and stored procedures.",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "source_id": {
                                "type": "string",
                                "description": "Database source ID (optional if single db configured)",
                            },
                            "object_type": {
                                "type": "string",
                                "enum": ["schema", "table", "column", "index", "procedure"],
                                "description": "Type of database object",
                            },
                            "schema": {
                                "type": "string",
                                "description": "Schema/database name to search within",
                            },
                            "table": {
                                "type": "string",
                                "description": "Table name (required when searching columns)",
                            },
                            "pattern": {
                                "type": "string",
                                "description": "Search pattern (supports % wildcard)",
                            },
                            "response_format": {
                                "type": "string",
                                "enum": ["markdown", "json"],
                                "default": "markdown",
                                "description": "Output format",
                            },
                        },
                    },
                ),
                Tool(
                    name="list_sources",
                    description="List all configured database connections with their types and status.",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "response_format": {
                                "type": "string",
                                "enum": ["markdown", "json"],
                                "default": "markdown",
                                "description": "Output format",
                            },
                        },
                    },
                ),
            ]

        @self.server.call_tool()
        async def call_tool(name: str, arguments: dict[str, Any]) -> list[TextContent]:
            """Handle tool calls."""
            logger.debug(f"Tool called: {name}", arguments)

            if name == "execute_sql":
                input_data = ExecuteSqlInput(
                    sql=arguments.get("sql", ""),
                    source_id=arguments.get("source_id"),
                    params=arguments.get("params"),
                    response_format=arguments.get("response_format", "markdown"),
                )
                result = await execute_sql(self.connector_manager, input_data)
                return [
                    TextContent(type="text", text=item["text"])
                    for item in result.content
                ]

            elif name == "search_objects":
                input_data = SearchObjectsInput(
                    source_id=arguments.get("source_id"),
                    object_type=arguments.get("object_type"),
                    schema=arguments.get("schema"),
                    table=arguments.get("table"),
                    pattern=arguments.get("pattern"),
                    response_format=arguments.get("response_format", "markdown"),
                )
                result = await search_objects(self.connector_manager, input_data)
                return [
                    TextContent(type="text", text=item["text"])
                    for item in result.content
                ]

            elif name == "list_sources":
                input_data = ListSourcesInput(
                    response_format=arguments.get("response_format", "markdown"),
                )
                result = await list_sources(self.connector_manager, input_data)
                return [
                    TextContent(type="text", text=item["text"])
                    for item in result.content
                ]

            else:
                return [TextContent(type="text", text=f"Unknown tool: {name}")]

        logger.info("Registered MCP tools: execute_sql, search_objects, list_sources")

    async def _connect_databases(self) -> None:
        """Try to connect to all configured databases."""
        try:
            await self.connector_manager.connect_all()
        except Exception as e:
            logger.warning(f"Some database connections failed: {e}")

    async def _start_stdio(self) -> None:
        """Start the server with stdio transport."""
        logger.info("MCP server starting with stdio transport")
        async with stdio_server() as (read_stream, write_stream):
            await self.server.run(
                read_stream,
                write_stream,
                self.server.create_initialization_options(),
            )

    async def _start_http(self) -> None:
        """Start the server with streamable HTTP transport (stateless mode)."""
        try:
            from starlette.applications import Starlette
            from starlette.middleware.cors import CORSMiddleware
            from starlette.responses import JSONResponse
            from starlette.routing import Route
            from mcp.server.streamable_http import StreamableHTTPServerTransport
            import uvicorn
            import asyncio
        except ImportError as e:
            raise ImportError(
                "HTTP transport requires additional dependencies. "
                "Install with: pip install starlette uvicorn"
            ) from e

        port = self.options.port

        # Create ASGI app class for MCP handling - Starlette requires this pattern
        # for raw ASGI handlers (plain async functions get wrapped differently)
        class MCPHandler:
            def __init__(self, mcp_server: Server):
                self.mcp_server = mcp_server

            async def __call__(self, scope, receive, send):
                """Handle MCP requests - creates new transport per request for stateless mode."""
                transport = StreamableHTTPServerTransport(
                    mcp_session_id=None,  # stateless
                    is_json_response_enabled=True,
                )

                async with transport.connect() as (read_stream, write_stream):
                    server_task = asyncio.create_task(
                        self.mcp_server.run(
                            read_stream,
                            write_stream,
                            self.mcp_server.create_initialization_options(),
                            stateless=True,  # Allow initialization from any node
                        )
                    )
                    try:
                        await transport.handle_request(scope, receive, send)
                    finally:
                        server_task.cancel()
                        try:
                            await server_task
                        except asyncio.CancelledError:
                            pass

        mcp_handler = MCPHandler(self.server)

        async def health_check(request: Any) -> JSONResponse:
            """Health check endpoint."""
            return JSONResponse({
                "status": "ok",
                "version": SERVER_VERSION,
                "sources": self.connector_manager.list_source_ids(),
            })

        app = Starlette(
            routes=[
                Route("/health", health_check, methods=["GET"]),
                Route("/mcp", mcp_handler, methods=["GET", "POST"]),
            ],
        )

        app.add_middleware(
            CORSMiddleware,
            allow_origins=["*"],
            allow_methods=["*"],
            allow_headers=["*"],
        )

        config = uvicorn.Config(
            app,
            host="0.0.0.0",
            port=port,
            log_level="warning",
        )
        self._http_server = uvicorn.Server(config)

        logger.info(f"MCP server starting with HTTP transport on port {port}")
        logger.info(f"MCP endpoint: http://0.0.0.0:{port}/mcp")
        logger.info(f"Health check: http://0.0.0.0:{port}/health")

        await self._http_server.serve()

    async def start(self) -> None:
        """Start the MCP server."""
        await self._connect_databases()

        if self.options.transport == "stdio":
            await self._start_stdio()
        else:
            await self._start_http()

    async def stop(self) -> None:
        """Stop the MCP server."""
        logger.info("Shutting down MCP server...")

        # Stop HTTP server if running
        if self._http_server:
            self._http_server.should_exit = True

        # Disconnect all databases
        await self.connector_manager.disconnect_all()

        logger.info("MCP server stopped")


async def create_server(options: ServerOptions) -> OpenDBServer:
    """Create and return an OpenDB MCP server instance."""
    return OpenDBServer(options)

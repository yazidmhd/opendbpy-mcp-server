#!/usr/bin/env python3
"""
OpenDB MCP Server - Main entry point.

Multi-database MCP server supporting PostgreSQL, MySQL, MariaDB,
SQLite, Hive, and Impala with Kerberos authentication.
"""

import argparse
import asyncio
import os
import signal
import sys
from typing import Optional

from .config import create_config_from_dsn, load_config
from .constants import SERVER_NAME, SERVER_VERSION
from .server import OpenDBServer, ServerOptions
from .utils.logger import logger


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        prog=SERVER_NAME,
        description="Multi-database MCP server supporting PostgreSQL, MySQL, MariaDB, SQLite, Hive, and Impala.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=f"""
USAGE:
  {SERVER_NAME} [OPTIONS]

OPTIONS:
  -c, --config <path>   Path to TOML configuration file
  -d, --dsn <dsn>       Database DSN for single-database mode
  -h, --help            Show this help message
  -v, --version         Show version number

ENVIRONMENT VARIABLES:
  TRANSPORT             Transport type: stdio (default) or http
  PORT                  HTTP port (default: 3000)
  LOG_LEVEL             Log level: debug, info, warn, error

EXAMPLES:
  # Using config file (stdio transport)
  {SERVER_NAME} --config opendb.toml

  # Single database via DSN
  {SERVER_NAME} --dsn "postgres://user:pass@localhost/db"

  # HTTP transport
  TRANSPORT=http PORT=3000 {SERVER_NAME} --config opendb.toml

CLAUDE DESKTOP INTEGRATION:
  Add to your Claude Desktop config:
  {{
    "mcpServers": {{
      "opendb": {{
        "command": "{SERVER_NAME}",
        "args": ["--config", "/path/to/opendb.toml"]
      }}
    }}
  }}

For more information, see: https://github.com/anthropics/opendb-mcp-server
""",
    )

    parser.add_argument(
        "-c",
        "--config",
        metavar="PATH",
        help="Path to TOML configuration file",
    )

    parser.add_argument(
        "-d",
        "--dsn",
        metavar="DSN",
        help="Database DSN for single-database mode",
    )

    parser.add_argument(
        "-v",
        "--version",
        action="version",
        version=f"{SERVER_NAME} v{SERVER_VERSION}",
    )

    # Also accept config path as positional argument
    parser.add_argument(
        "config_path",
        nargs="?",
        metavar="CONFIG",
        help="Path to TOML configuration file (alternative to --config)",
    )

    return parser.parse_args()


async def run_server(server: OpenDBServer) -> None:
    """Run the server with signal handling."""
    loop = asyncio.get_running_loop()
    stop_event = asyncio.Event()

    def signal_handler() -> None:
        logger.info("Received shutdown signal")
        stop_event.set()

    # Set up signal handlers (only on Unix-like systems)
    if sys.platform != "win32":
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, signal_handler)

    try:
        # Start server in background task
        server_task = asyncio.create_task(server.start())

        # Wait for either server to complete or stop signal
        done, pending = await asyncio.wait(
            [server_task, asyncio.create_task(stop_event.wait())],
            return_when=asyncio.FIRST_COMPLETED,
        )

        # Cancel pending tasks
        for task in pending:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

    except KeyboardInterrupt:
        logger.info("Interrupted by user")
    finally:
        await server.stop()


def main() -> None:
    """Main entry point."""
    args = parse_args()

    # Determine config source
    config_path = args.config or args.config_path
    dsn = args.dsn

    if not config_path and not dsn:
        print("Error: Either --config or --dsn is required", file=sys.stderr)
        print("Run with --help for usage information", file=sys.stderr)
        sys.exit(1)

    try:
        # Load configuration
        if config_path:
            logger.info(f"Loading configuration from {config_path}")
            config = load_config(config_path)
        elif dsn:
            logger.info("Using single-database mode with DSN")
            config = create_config_from_dsn(dsn)
        else:
            raise ValueError("No configuration provided")

        logger.info(f"Configured {len(config.sources)} database source(s)")

        # Determine transport from environment
        transport_env = os.environ.get("TRANSPORT", "stdio").lower()
        transport = "http" if transport_env == "http" else "stdio"
        port = int(os.environ.get("PORT", "3000"))

        # Create server
        server = OpenDBServer(
            ServerOptions(
                config=config,
                transport=transport,  # type: ignore
                port=port,
            )
        )

        # Run server
        asyncio.run(run_server(server))

    except KeyboardInterrupt:
        logger.info("Interrupted by user")
        sys.exit(0)

    except Exception as e:
        logger.error("Failed to start server", e)
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()

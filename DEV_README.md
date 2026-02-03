# Developer Guide for OpenDB MCP Server

Welcome! This guide explains what each file in this project does, written for developers who are new to Python or MCP servers.

## What is This Project?

This is an **MCP (Model Context Protocol) server** that lets AI assistants (like Claude) connect to and query databases. Think of it as a translator between the AI and your databases.

**Supported databases:**
- PostgreSQL
- MySQL / MariaDB
- Apache Hive
- Apache Impala

## How Data Flows Through the System

```
┌──────────────┐     ┌──────────────┐     ┌───────────────┐     ┌──────────────┐
│  AI Client   │────▶│  MCP Server  │────▶│   Connector   │────▶│   Database   │
│  (Claude)    │◀────│  (server.py) │◀────│  (postgres.py)│◀────│  (PostgreSQL)│
└──────────────┘     └──────────────┘     └───────────────┘     └──────────────┘
     request              route               execute              query
     response            format               results              data
```

1. The AI sends a request (like "run this SQL query")
2. The server routes it to the right tool
3. The tool uses a connector to talk to the database
4. Results flow back up the chain

---

## How to Run the Server

### Quick Start

```bash
# Using a config file
python -m opendb_mcp --config config.toml

# Using a single database DSN
python -m opendb_mcp --dsn "postgres://user:pass@localhost/mydb"
```

### Environment Variables

```bash
TRANSPORT=stdio    # or "http" for web transport
PORT=3000          # HTTP port (only for http transport)
LOG_LEVEL=info     # debug, info, warn, error
```

See `examples/example-config.toml` for configuration options.

---

## Project Structure

```
src/opendb_mcp/
├── __main__.py          # Entry point - where the program starts
├── server.py            # MCP server setup and tool routing
├── constants.py         # Shared values (limits, defaults, etc.)
│
├── config/              # Configuration handling
│   ├── __init__.py      # Package exports
│   ├── types.py         # Data models for config (blueprints)
│   ├── loader.py        # Reads TOML config files
│   └── keytab.py        # Kerberos keytab file handling
│
├── connectors/          # Database connections
│   ├── __init__.py      # Connector factory/manager
│   ├── base.py          # Template all connectors follow
│   ├── postgres.py      # PostgreSQL connector
│   ├── mysql.py         # MySQL/MariaDB connector
│   ├── hive.py          # Apache Hive connector
│   └── impala.py        # Apache Impala connector
│
├── tools/               # MCP tools (what the AI can do)
│   ├── __init__.py      # Package exports
│   ├── execute_sql.py   # Run SQL queries
│   ├── search_objects.py# Browse database schemas
│   └── list_sources.py  # List configured databases
│
├── services/            # Background services
│   ├── __init__.py      # Package exports
│   └── kerberos.py      # Kerberos ticket management
│
└── utils/               # Helper utilities
    ├── __init__.py      # Package exports
    ├── formatters.py    # Format output as Markdown/JSON
    ├── errors.py        # Custom error types
    └── logger.py        # Logging to stderr
```

---

## File-by-File Explanations

### Entry Points

#### `__main__.py` - Where the Program Starts

This file runs when you execute `python -m opendb_mcp`. It:

1. Parses command-line arguments (`--config`, `--dsn`)
2. Loads the configuration
3. Creates and starts the server
4. Handles shutdown signals (Ctrl+C)

```python
# Key function - the main entry point
def main() -> None:
    args = parse_args()
    config = load_config(args.config)  # Load settings
    server = OpenDBServer(...)          # Create server
    asyncio.run(run_server(server))     # Start it
```

#### `server.py` - The MCP Server Setup

This is the heart of the application. It:

1. Creates an MCP server instance
2. Registers available tools (execute_sql, search_objects, list_sources)
3. Routes incoming tool calls to the right handler
4. Manages database connections

```python
# The main server class
class OpenDBServer:
    def __init__(self, options: ServerOptions):
        self.connector_manager = ConnectorManager(options.config)
        self.server = Server(SERVER_NAME)
        self._setup_handlers()  # Register tools
```

**Analogy:** Think of this as a receptionist who knows which department to send each request to.

#### `constants.py` - Shared Values Used Everywhere

Stores values that multiple files need access to:

```python
CHARACTER_LIMIT = 100_000   # Max response size
DEFAULT_MAX_ROWS = 1000     # Max rows returned
DEFAULT_QUERY_TIMEOUT = 30  # Query timeout (seconds)
SERVER_NAME = "opendb-mcp"
```

---

### config/ folder - Configuration Handling

#### `types.py` - Data Models (Blueprints for Config)

Defines what a valid configuration looks like using Pydantic models. These are like blueprints that describe the shape of data.

```python
# Example: What a database source configuration looks like
class HostBasedSourceConfig(BaseSourceConfig):
    host: str                    # Required: database host
    port: Optional[int] = None   # Optional: defaults to None
    database: Optional[str] = None
    user: Optional[str] = None
    password: Optional[str] = None
```

**Why Pydantic?** It automatically validates data and gives helpful error messages if the config is wrong.

#### `loader.py` - Reads the TOML Config File

Takes a config file path and returns a validated configuration object. It:

1. Reads the TOML file
2. Substitutes environment variables (`${VAR_NAME}`)
3. Validates against the Pydantic models
4. Returns a `ParsedConfig` object

```python
def load_config(config_path: str) -> ParsedConfig:
    # Read file, substitute env vars, validate, return config
```

#### `keytab.py` - Handles Kerberos Authentication Files

Kerberos is an enterprise authentication system. A "keytab" is a file containing credentials.

This file:
1. Decodes base64-encoded keytab content (useful for storing in env vars)
2. Writes keytab files with secure permissions
3. Validates keytab files exist

#### `__init__.py` - Makes This Folder a Python Package

Exports all the public classes/functions so other files can import them:

```python
from .loader import load_config
from .types import ParsedConfig, Settings
# Now other files can do: from config import load_config
```

---

### connectors/ folder - Database Connections

#### `base.py` - The Template All Database Connectors Follow

Defines an **abstract base class** - a template that all database connectors must follow. This ensures consistency.

```python
class BaseConnector(ABC):
    @abstractmethod
    async def connect(self) -> None:
        """Every connector must implement this"""
        pass

    @abstractmethod
    async def execute(self, sql: str, ...) -> QueryResult:
        """Every connector must implement this"""
        pass
```

**Analogy:** Think of this as a job description. Every connector "employee" must be able to do these things, but they can do them their own way.

Key features:
- `execute()` - Runs SQL with read-only enforcement
- `search_objects()` - Finds schemas, tables, columns
- `_is_write_query()` - Checks if query modifies data

#### `postgres.py` - PostgreSQL Database Connector

Implements `BaseConnector` for PostgreSQL using the `asyncpg` library.

```python
class PostgresConnector(BaseConnector):
    async def connect(self) -> None:
        # Create connection pool to PostgreSQL
        self._pool = await asyncpg.create_pool(...)

    async def _execute_query(self, sql, ...) -> QueryResult:
        # Run query and return results
```

#### `mysql.py` - MySQL/MariaDB Connector

Similar to PostgreSQL connector but uses `aiomysql` library. Handles both MySQL and MariaDB.

#### `hive.py` and `impala.py` - Apache Connectors

Connect to Hadoop ecosystem databases. Support Kerberos authentication for secure enterprise environments.

#### `__init__.py` - Connector Factory/Manager

The `ConnectorManager` class:
1. Creates the right connector type based on config
2. Keeps track of all connectors
3. Resolves which connector to use for a request

```python
class ConnectorManager:
    def resolve(self, source_id: str | None) -> BaseConnector:
        """Get the right connector for a request"""
        if source_id:
            return self._connectors[source_id]
        return self.get_default()  # Use default if only one configured
```

---

### tools/ folder - MCP Tools (What the AI Can Do)

These are the capabilities exposed to the AI.

#### `execute_sql.py` - Run SQL Queries

The main tool - lets the AI run SQL queries.

```python
@dataclass
class ExecuteSqlInput:
    sql: str                     # The SQL to run
    source_id: Optional[str]     # Which database (optional if only one)
    params: Optional[list]       # Prepared statement parameters
    response_format: str         # "markdown" or "json"

async def execute_sql(connector_manager, input_data) -> ExecuteSqlResult:
    connector = connector_manager.resolve(input_data.source_id)
    result = await connector.execute(input_data.sql, ...)
    return format_query_results(result)
```

#### `search_objects.py` - Browse Database Schemas

Lets the AI explore what's in the database: schemas, tables, columns, indexes.

```python
# What can be searched
object_type: "schema" | "table" | "column" | "index" | "procedure"
```

#### `list_sources.py` - List Configured Databases

Simple tool that returns all configured database sources with their status.

---

### services/ folder

#### `kerberos.py` - Kerberos Ticket Management

Handles Kerberos authentication for secure Hive/Impala connections:

1. Runs `kinit` to get a Kerberos ticket
2. Tracks when tickets expire
3. Automatically refreshes tickets before they expire

```python
class KerberosAuth:
    async def initialize(self) -> None:
        await self._kinit(keytab_path, principal)  # Get ticket
        self._schedule_refresh()  # Auto-refresh before expiry
```

---

### utils/ folder - Helper Utilities

#### `formatters.py` - Format Output as Markdown/JSON

Converts query results into readable formats:

```python
# Query results become Markdown tables:
# | id | name  | email           |
# |----|-------|-----------------|
# | 1  | Alice | alice@email.com |

# Or JSON:
# {"columns": ["id", "name"], "rows": [...]}
```

#### `errors.py` - Custom Error Types

Defines specific error classes for better error handling:

```python
class ConnectionError(DatabaseError):
    """When we can't connect to a database"""

class QueryError(DatabaseError):
    """When a SQL query fails"""

class KerberosError(Exception):
    """When Kerberos authentication fails"""
```

#### `logger.py` - Logging to stderr

A custom logger that writes to stderr (not stdout). This is important because MCP uses stdout for communication, so logs must go elsewhere.

```python
# Logs look like:
# [2024-01-15T10:30:45] INFO  Connected to PostgreSQL: mydb
```

---

## Key Python Concepts Used

### `async/await` - Asynchronous Programming

This project uses async/await extensively. Here's what it means:

```python
# Without async - blocks while waiting
def query_database():
    result = database.execute(sql)  # Program stops here
    return result

# With async - can do other things while waiting
async def query_database():
    result = await database.execute(sql)  # Can handle other requests
    return result
```

**Why?** Database queries are slow. Async lets the server handle multiple requests instead of waiting idle.

### `dataclass` - Simple Data Containers

A shortcut for creating classes that just hold data:

```python
# Instead of this:
class Person:
    def __init__(self, name, age):
        self.name = name
        self.age = age

# You can write this:
@dataclass
class Person:
    name: str
    age: int
```

### Pydantic Models - Config Validation

Like dataclasses but with automatic validation:

```python
class Settings(BaseModel):
    max_rows: int = Field(1000, ge=1, le=100000)  # Must be 1-100000

Settings(max_rows=50000)   # OK
Settings(max_rows=999999)  # Error! Too big
```

### Abstract Base Classes - Template Pattern

When you want to ensure different classes have the same methods:

```python
class BaseConnector(ABC):
    @abstractmethod
    async def connect(self) -> None:
        pass  # Subclasses MUST implement this

class PostgresConnector(BaseConnector):
    async def connect(self) -> None:
        # PostgreSQL-specific connection logic
```

### Type Hints - What `def foo(x: str) -> int` Means

Type hints document what types a function expects and returns:

```python
def greet(name: str) -> str:
    #         ↑ expects string    ↑ returns string
    return f"Hello, {name}"

def add(a: int, b: int) -> int:
    return a + b

# Optional means "this type or None"
def find_user(id: int) -> Optional[User]:
    # Returns User or None
```

---

## Code Flow Walkthrough

### 1. Server Starts Up

```
python -m opendb_mcp --config myconfig.toml
         │
         ▼
    __main__.py
         │
         ├── parse_args() - Read command line
         ├── load_config() - Load & validate TOML
         │       │
         │       └── types.py - Validate against models
         │
         ├── OpenDBServer() - Create server
         │       │
         │       ├── ConnectorManager() - Create db connectors
         │       │       │
         │       │       └── PostgresConnector(), etc.
         │       │
         │       └── _setup_handlers() - Register MCP tools
         │
         └── server.start() - Begin listening
                 │
                 ├── connect_all() - Connect to databases
                 └── stdio_server() - Listen for requests
```

### 2. User Runs a SQL Query

```
AI: "execute_sql with sql='SELECT * FROM users'"
         │
         ▼
    server.py - call_tool("execute_sql", {...})
         │
         ▼
    execute_sql.py - execute_sql(connector_manager, input)
         │
         ├── connector_manager.resolve() - Get right connector
         │
         ├── connector.execute(sql) - Run query
         │       │
         │       ├── _is_write_query() - Check if allowed
         │       └── _execute_query() - Actually run it
         │
         └── format_query_results() - Format as Markdown
                 │
                 ▼
    Response: "| id | name | email |..."
```

---

## Common Tasks

### Adding a New Database Type

1. Create `src/opendb_mcp/connectors/newdb.py`
2. Extend `BaseConnector` and implement all abstract methods
3. Add to `ConnectorManager._create_connector()` in `connectors/__init__.py`
4. Add to `DATABASE_TYPES` in `constants.py`
5. Add config type to `config/types.py` if needed

### Adding a New MCP Tool

1. Create `src/opendb_mcp/tools/my_tool.py`
2. Define input/output dataclasses
3. Implement the async function
4. Register in `server.py` `_setup_handlers()`
5. Export from `tools/__init__.py`

---

## Getting Help

- Check `examples/example-config.toml` for configuration examples
- Run with `LOG_LEVEL=debug` for verbose logging
- Errors include source IDs to help identify which database failed

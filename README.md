# OpenDB MCP Server (Python)

A multi-database MCP (Model Context Protocol) server for Python, supporting PostgreSQL, MySQL/MariaDB, SQLite, Apache Hive, and Apache Impala with Kerberos authentication.

## Supported Databases

| Database      | Driver      |
| ------------- | ----------- |
| PostgreSQL    | `asyncpg`   |
| MySQL         | `aiomysql`  |
| MariaDB       | `aiomysql`  |
| SQLite        | `aiosqlite` |
| Apache Hive   | `pyhive`    |
| Apache Impala | `pyhive`    |

## Prerequisites

- Python >= 3.9
- uv or pip

## Quick Start (From Source)

### 1. Clone the Repository

```bash
git clone https://github.com/anthropics/opendb-mcp-server
cd opendbpy-mcp-server
```

### 2. Create Virtual Environment

```bash
# Using uv (recommended, faster)
uv venv

# Using python
python3 -m venv .venv
```

### 3. Activate Virtual Environment

```bash
# macOS/Linux
source .venv/bin/activate

# Windows
.venv\Scripts\activate
```

### 4. Install Dependencies

```bash
# Using uv (recommended, faster)
uv pip install -e ".[all]"

# Using pip
pip install -e ".[all]"
```

### 5. Configure Database

Copy the example config and edit with your database credentials:

```bash
cp examples/mysql-local.toml myconfig.toml
```

Example MySQL configuration:

```toml
[settings]
readonly = false
max_rows = 1000

[[sources]]
id = "mysql-local"
type = "mysql"
host = "localhost"
port = 3306
database = "mydb"
user = "root"
password = "yourpassword"
```

### 6. Choose Transport Method

Choose one of the following methods to run the server:

---

## Option A: HTTP Transport

HTTP runs the server in the background. Good for shared/remote access.

### Find Your IP Address

```bash
# macOS
ipconfig getifaddr en0

# Linux
hostname -I | awk '{print $1}'
```

### Setup

1. Edit `start.sh` and update the `--config` path:

```bash
CONFIG="/path/to/your/config.toml"
```

2. Create logs directory:

```bash
sudo mkdir -p /devlogs && sudo chown $USER /devlogs
```

### Start/Stop Server

```bash
# Start
./start.sh

# Stop
./stop.sh
```

Logs are stored in `/devlogs/opendbpy-{timestamp}.log`.

### Add to Claude Code (HTTP)

```bash
claude mcp add opendb --transport http --url http://192.168.1.51:3000/mcp
```

### Test with MCP Inspector (HTTP)

```bash
npx @modelcontextprotocol/inspector
```

Then enter `http://192.168.1.51:3000/mcp` in the UI.

---

## Option B: Stdio Transport

Stdio spawns the server as a subprocess. Simpler setup, no background process needed.

### Add to Claude Code (Stdio)

```bash
claude mcp add opendb -- python3 -m opendb_mcp --config /path/to/config.toml
```

Or if installed globally:

```bash
claude mcp add opendb -- opendb-mcp-server --config /path/to/config.toml
```

### Add to Claude Desktop (Stdio)

Add to config (`~/Library/Application Support/Claude/claude_desktop_config.json`):

```json
{
  "mcpServers": {
    "opendb": {
      "command": "python3",
      "args": ["-m", "opendb_mcp", "--config", "/path/to/config.toml"]
    }
  }
}
```

Or using the installed command:

```json
{
  "mcpServers": {
    "opendb": {
      "command": "opendb-mcp-server",
      "args": ["--config", "/path/to/config.toml"]
    }
  }
}
```

### Test with MCP Inspector (Stdio)

```bash
npx @modelcontextprotocol/inspector python3 -m opendb_mcp --config /path/to/config.toml
```

---

## Configuration

### Global Settings

```toml
[settings]
readonly = false          # Prevent all write operations
max_rows = 1000           # Maximum rows returned per query
query_timeout = 30        # Query timeout in seconds
connection_timeout = 10   # Connection timeout in seconds
```

### Environment Variables in Config

Use `${VAR_NAME}` or `${VAR_NAME:-default}`:

```toml
[[sources]]
id = "mysql-prod"
type = "mysql"
host = "${MYSQL_HOST:-localhost}"
password = "${MYSQL_PASSWORD}"
```

### Environment Variables

| Variable    | Default | Description                                 |
| ----------- | ------- | ------------------------------------------- |
| `TRANSPORT` | stdio   | Transport type: `stdio` or `http`           |
| `PORT`      | 3000    | HTTP server port                            |
| `LOG_LEVEL` | info    | Log level: `debug`, `info`, `warn`, `error` |

### Source Types

#### PostgreSQL / MySQL / MariaDB

```toml
[[sources]]
id = "my-db"
type = "postgres"  # or "mysql", "mariadb"

# DSN-based
dsn = "postgres://user:pass@host:5432/db"

# OR host-based
host = "localhost"
port = 5432
database = "mydb"
user = "user"
password = "pass"
ssl = true
```

#### SQLite

```toml
[[sources]]
id = "local-db"
type = "sqlite"
path = "./data/app.db"  # or ":memory:"
```

#### Hive / Impala

```toml
[[sources]]
id = "hive-prod"
type = "hive"  # or "impala"
host = "hiveserver.example.com"
port = 10000
database = "default"
auth_mechanism = "KERBEROS"  # or "NONE", "PLAIN"
principal = "hive/_HOST@REALM"
keytab = "/etc/keytabs/user.keytab"
user_principal = "user@REALM"
```

## MCP Tools

### `execute_sql`

Execute SQL queries against configured databases.

```json
{
  "sql": "SELECT * FROM users LIMIT 10",
  "params": [],
  "response_format": "markdown"
}
```

### `search_objects`

Explore database schemas.

```json
{ "object_type": "schema" }
{ "object_type": "table", "schema": "mydb" }
{ "object_type": "column", "schema": "mydb", "table": "users" }
```

### `list_sources`

List all configured database connections.

## Removing from Claude Code

```bash
claude mcp remove opendb
```

## Development

```bash
# Clone and install
git clone https://github.com/anthropics/opendb-mcp-server
cd opendbpy-mcp-server

# Using uv (faster)
uv pip install -e ".[dev,all]"

# Using pip
pip install -e ".[dev,all]"

# Run tests
pytest

# Type checking
mypy src/

# Linting
ruff check src/
```

## Architecture

```
opendbpy-mcp-server/
├── src/opendb_mcp/
│   ├── __main__.py           # CLI entry point
│   ├── server.py             # MCP server (stdio + HTTP)
│   ├── constants.py          # Shared constants
│   ├── config/               # Configuration loading
│   │   ├── types.py          # Pydantic models
│   │   └── loader.py         # TOML + env var substitution
│   ├── connectors/           # Database connectors
│   │   ├── base.py           # Abstract base class
│   │   ├── postgres.py       # PostgreSQL (asyncpg)
│   │   ├── mysql.py          # MySQL/MariaDB (aiomysql)
│   │   ├── sqlite.py         # SQLite (aiosqlite)
│   │   ├── hive.py           # Hive (pyhive)
│   │   └── impala.py         # Impala (pyhive)
│   ├── tools/                # MCP tools
│   │   ├── execute_sql.py
│   │   ├── search_objects.py
│   │   └── list_sources.py
│   ├── services/
│   │   └── kerberos.py       # Kerberos auth
│   └── utils/
│       ├── logger.py         # Stderr logging
│       ├── formatters.py     # Markdown/JSON output
│       └── errors.py         # Custom exceptions
├── examples/
│   ├── mysql-local.toml      # MySQL HTTP example
│   ├── mysql-stdio.toml      # MySQL stdio example
│   └── sqlite-test.toml      # SQLite test example
├── start.sh                  # Start HTTP server
└── stop.sh                   # Stop HTTP server
```

## License

MIT

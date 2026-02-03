# OpenDB MCP Server (Python)

A multi-database MCP (Model Context Protocol) server for Python, supporting PostgreSQL, MySQL/MariaDB, Apache Hive, and Apache Impala with Kerberos authentication.

## Supported Databases

| Database      | Driver     |
| ------------- | ---------- |
| PostgreSQL    | `asyncpg`  |
| MySQL         | `aiomysql` |
| MariaDB       | `aiomysql` |
| Apache Hive   | `pyhive`   |
| Apache Impala | `pyhive`   |

## Prerequisites

- Python >= 3.10
- uv or pip

## Quick Start (From Source)

### 1. Clone the Repository

```bash
git clone https://github.com/anthropics/opendb-mcp
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

# Or install from requirements.txt (for PCF/production)
pip install -r requirements.txt
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

HTTP runs the server as a web service. Good for shared/remote access.

### Start Server

```bash
# Run in foreground
TRANSPORT=http python3 -m opendb_mcp --config config/opendb.toml

# Run in background
TRANSPORT=http python3 -m opendb_mcp --config config/opendb.toml &

# Run on custom port (default: 3000)
TRANSPORT=http PORT=8080 python3 -m opendb_mcp --config config/opendb.toml
```

### Stop Server

```bash
# If running in background, find and kill the process
pkill -f "opendb_mcp"
```

### Add to Claude Code (HTTP)

```bash
claude mcp add opendb --transport http --url http://localhost:3000/mcp
```

### Test with MCP Inspector (HTTP)

```bash
npx @modelcontextprotocol/inspector
```

Then enter `http://localhost:3000/mcp` in the UI.

---

## Option B: Stdio Transport

Stdio spawns the server as a subprocess. Simpler setup, no background process needed.

### Add to Claude Code (Stdio)

```bash
claude mcp add opendb -- python3 -m opendb_mcp --config /path/to/config.toml
```

Or if installed globally:

```bash
claude mcp add opendb -- opendb-mcp --config /path/to/config.toml
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
      "command": "opendb-mcp",
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
git clone https://github.com/anthropics/opendb-mcp
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
│   └── mysql-stdio.toml      # MySQL stdio example
├── start.sh                  # Start HTTP server
└── stop.sh                   # Stop HTTP server
```

## PCF Deployment

This server is designed for deployment on Pivotal Cloud Foundry (PCF).

### Prerequisites

- PCF CLI (`cf`) installed and logged in
- Database service instance bound or external database accessible

### Files for PCF

- `manifest.yml` - PCF application manifest
- `requirements.txt` - Python dependencies (used by PCF buildpack)
- `runtime.txt` - Python version specification
- `config/` - Configuration directory

### Deploy to PCF

1. **Update configuration**: Edit `config/opendb.toml` with your database credentials (use environment variables for secrets):

```toml
[[sources]]
id = "prod-db"
type = "postgres"
host = "${DB_HOST}"
port = 5432
database = "${DB_NAME}"
user = "${DB_USER}"
password = "${DB_PASSWORD}"
```

2. **Push the application**:

```bash
cf push
```

3. **Set environment variables**:

```bash
cf set-env opendb-mcp DB_HOST your-db-host
cf set-env opendb-mcp DB_NAME your-db-name
cf set-env opendb-mcp DB_USER your-db-user
cf set-env opendb-mcp DB_PASSWORD your-db-password
cf restage opendb-mcp
```

### Health Check

The server exposes a `/health` endpoint for PCF health monitoring.

```bash
curl https://opendb-mcp.your-pcf-domain.com/health
```

## License

MIT

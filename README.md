# MCP MySQL Server

[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.104+-green.svg)](https://fastapi.tiangolo.com/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Docker](https://img.shields.io/badge/docker-ready-blue.svg)](https://www.docker.com/)
[![Tests](https://img.shields.io/badge/tests-passing-brightgreen.svg)](#testing)

A production-ready **Model Context Protocol (MCP) server** for MySQL database operations. Provides secure HTTP endpoints for executing read-only queries, analyzing database performance, and monitoring MySQL server status.

## 🚀 Quick Start (1 minute)

### Option 1: Docker (Recommended)

```bash
# Clone and start with Docker Compose
git clone <repository-url>
cd mcp-mysql-server
cp .env.example .env
docker-compose up -d

# Test the server
curl http://localhost:3000/health
```

### Option 2: Local Installation

```bash
# Install and run locally
pip install -e .
python server.py

# Test the server
curl http://localhost:3000/health
```

## 📋 Features

### 🔧 Core Tools

| Tool | Description | Input | Output |
|------|-------------|-------|--------|
| `ping` | Health check utility | `echo: string` | `{ok, echo, timestamp}` |
| `mysql_query` | Execute read-only SQL queries | `query: string` | `{columns, rows, row_count}` |
| `mysql_status` | Get MySQL server status | None | `{status_variables}` |
| `mysql_innodb_metrics` | Analyze InnoDB performance | None | `{metrics, analysis}` |

### 🛡️ Security Features

- **Authentication**: Bearer token, API key, or no auth
- **Rate limiting**: Configurable per-IP limits
- **Input validation**: Pydantic schema validation
- **Read-only queries**: Prevents data modification
- **Request size limits**: Configurable payload limits
- **CORS support**: Configurable cross-origin requests

### 📊 Observability

- **Structured logging**: JSON format with request tracing
- **Health monitoring**: `/health` endpoint with metrics
- **Error tracking**: Comprehensive error handling
- **Performance metrics**: Request duration and counts

## 🔧 Configuration

### Environment Variables

```bash
# Server Configuration
PORT=3000                    # Server port
HOST=0.0.0.0                # Server host
AUTH_MODE=none              # none, bearer, api_key
AUTH_TOKEN=your-token       # Authentication token

# MySQL Configuration
MYSQL_HOST=localhost        # MySQL server host
MYSQL_PORT=3306            # MySQL server port
MYSQL_USER=root            # MySQL username
MYSQL_PASSWORD=password    # MySQL password
MYSQL_DATABASE=test        # Default database

# Rate Limiting
RATE_LIMIT_REQUESTS_PER_MINUTE=60  # Requests per minute per IP
RATE_LIMIT_BURST=10               # Burst allowance

# Security
REQUEST_TIMEOUT_SECONDS=30        # Request timeout
MAX_REQUEST_SIZE_MB=10           # Max request size
```

## 📡 API Endpoints

### Health Check
```http
GET /health
```
```json
{
  "status": "ok",
  "version": "0.1.0",
  "uptime_seconds": 3600,
  "tools": ["ping", "mysql_query", "mysql_status", "mysql_innodb_metrics"],
  "metrics": {
    "total_requests": 150,
    "total_errors": 2,
    "recent_errors": []
  }
}
```

### List Tools
```http
GET /tools
```
```json
{
  "ping": {
    "description": "Health/ping utility returning timestamp and echo text",
    "input_schema": {...},
    "output_schema": {...}
  }
}
```

### Invoke Tool
```http
POST /invoke
Content-Type: application/json
Authorization: Bearer your-token  # If auth enabled

{
  "tool": "mysql_query",
  "args": {
    "query": "SELECT COUNT(*) as user_count FROM users"
  }
}
```

```json
{
  "columns": ["user_count"],
  "rows": [[42]],
  "row_count": 1,
  "execution_time_ms": 15
}
```

## 🐳 Docker Deployment

### Development
```bash
docker-compose up -d
```

### Production
```bash
# Build production image
docker build -t mcp-mysql-server .

# Run with custom configuration
docker run -d \
  -p 3000:3000 \
  -e AUTH_MODE=bearer \
  -e AUTH_TOKEN=your-secure-token \
  -e MYSQL_HOST=your-mysql-host \
  mcp-mysql-server
```

## 🧪 Testing

```bash
# Install development dependencies
pip install -e ".[dev]"

# Run tests
pytest

# Run with coverage
pytest --cov=. --cov-report=html

# Run specific test categories
pytest tests/test_ping.py -v
pytest tests/test_http.py -v
pytest tests/test_auth.py -v
```

## Installation

Install required packages:

```bash
pip install -r requirements.txt
```

Or install key dependencies separately:

```bash
pip install "mcp[cli]"
pip install mysql-connector-python
```

## Configuration

Configure via environment variables:

| Variable         | Description            | Default   |
| ---------------- | ---------------------- | --------- |
| `MYSQL_HOST`     | MySQL server host      | `localhost` |
| `MYSQL_PORT`     | MySQL server port      | `3306`    |
| `MYSQL_USER`     | MySQL username         | `root`    |
| `MYSQL_PASSWORD` | MySQL password         | (empty)   |
| `MYSQL_DATABASE` | Default database name  | (empty)   |

Example (Linux/macOS):

```bash
export MYSQL_HOST=localhost
export MYSQL_PORT=3306
export MYSQL_USER=myuser
export MYSQL_PASSWORD=mypassword
export MYSQL_DATABASE=mydatabase
```

Example (Windows PowerShell):

```powershell
$env:MYSQL_HOST = "localhost"
$env:MYSQL_PORT = "3306"
$env:MYSQL_USER = "myuser"
$env:MYSQL_PASSWORD = "mypassword"
$env:MYSQL_DATABASE = "mydatabase"
```

## Usage

Run the server:

- Default stdio transport:

```bash
python mysql_server.py
```

- SSE transport (for web clients):

```bash
python mysql_server.py --transport sse
```

Get help:

```bash
python mysql_server.py --help
```

## Integration

To integrate with Claude Desktop, update your config file (`%APPDATA%/Claude/claude_desktop_config.json` on Windows or `~/Library/Application Support/Claude/claude_desktop_config.json` on macOS):

```json
{
  "mcpServers": {
    "mysql": {
      "command": "python",
      "args": ["path/to/mysql_server.py"],
      "env": {
        "MYSQL_HOST": "localhost",
        "MYSQL_PORT": "3306",
        "MYSQL_USER": "your_username",
        "MYSQL_PASSWORD": "your_password",
        "MYSQL_DATABASE": "your_database"
      }
    }
  }
}
```

## Example Workflows

- List all tables: use `list_tables` tool or access `mysql://tables` resource.
- Inspect table schema: use `describe_table` tool or `mysql://schema/{table_name}`.
- Execute queries: use `execute_sql` for select or data modification queries.
- Analyze slow queries and deadlocks.
- Audit user privileges and monitor SSL/TLS connections.
- Monitor replication lag and binary logs for health.

## Included Tools

The server includes a rich set of tools such as:

- mysql_query, list_mysql_tables, mysql_table_schema, mysql_table_data
- mysql_table_indexes, mysql_table_size, mysql_table_status
- mysql_fragmentation_analysis, mysql_index_optimization_suggestions, mysql_slow_query_analysis
- mysql_deadlock_detection, mysql_buffer_pool_cache_diagnostics
- mysql_user_privileges, mysql_create_user, mysql_drop_user, mysql_change_user_password
- mysql_backup_health_check, mysql_replication_lag_monitoring, mysql_ssl_tls_configuration_audit
- mysql_server_health_dashboard, mysql_performance_recommendations
- mysql_event_scheduler, mysql_partition_management_recommendations
- And many more diagnostic, operational, and security tools.

## Security Considerations

- Use secure connections when possible.
- Store credentials in environment variables.
- Only use the server in trusted environments or behind network security.
- All queries are validated for safety, but always review for injection risks.
- The server includes comprehensive privilege auditing tools.

## Error Handling

- Robust error reporting for connection, syntax, permission, and network errors.
- Structured error response format for easy automated handling.

## Development

Project structure:

```
mcp-mysql-server/
├── mysql_server.py      # Core server code with tools and protocols
├── requirements.txt     # Python dependencies
├── README.md            # This documentation
└── pyproject.toml       # Optional project metadata
```

### Testing

- Ensure MySQL server running and reachable.
- Configure environment variables.
- Run `python mysql_server.py`
- Optionally test with `mcp dev mysql_server.py`

## Contributing

- Fork repository
- Create branches for features or fixes
- Add tests and documentation
- Submit pull requests for review

## License

MIT License — Open source and free to use

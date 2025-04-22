## Developing

### Setup

```bash
make setup

# <authentication is the same as in production>
```

### Run using the MCP inspector

```bash
source .venv/bin/activate
mcp dev src/mcp_server_datahub/mcp_server.py
```

### Run tests

The test suite is currently very simplistic, and requires a live DataHub instance.

```bash
make test
```

## Publishing

```bash
export UV_PUBLISH_TOKEN=...
make publish
```

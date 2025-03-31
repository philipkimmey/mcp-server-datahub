## Developing

### Setup

```bash
uv sync --no-sources
# Alternatively, if also developing on acryl-datahub:
# Assumes the datahub repo is checked out at ../datahub
uv sync

# <authentication is the same as in production>
```

### Run using the MCP inspector

```bash
source .venv/bin/activate
mcp dev mcp_server.py
```

### Run tests

The test suite is currently very simplistic, and requires a live DataHub instance.

```bash
pytest
```

## Publishing

```bash
uv build
uv publish
```

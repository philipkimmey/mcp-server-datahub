# mcp-server-datahub

### Setup

```bash
uv sync --no-sources
# Alternatively, if also developing on acryl-datahub:
# Assumes the datahub repo is checked out at ../datahub
uv sync


datahub init  # configure datahub token
```

### Run in dev mode

```bash
source .venv/bin/activate
mcp dev mcp_server.py
```

### MCP server config

```
command: <path>/mcp-server-datahub/.venv/bin/mcp
args:
    run
    <path>/mcp-server-datahub/mcp_server.py
```

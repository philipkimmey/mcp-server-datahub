# mcp-server-datahub

A [Model Context Protocol](https://modelcontextprotocol.io/) server implementation for [DataHub](https://datahubproject.io/).
This enables AI agents to query DataHub for metadata and context about your data ecosystem.

Supports both DataHub OSS and DataHub Cloud.

## Features

- Searching across all entity types and using arbitrary filters
- Fetching metadata for any entity
- Traversing the lineage graph, both upstream and downstream
- Listing SQL queries associated with a dataset

## Usage

```bash
uv sync --no-sources
```

For authentication, you can either use `datahub init` to configure a global `~/.datahubenv` file, or you can set the appropriate environment variables:

```bash
datahub init   # follow the prompts

# Alternatively, use these environment variables:
export DATAHUB_GMS_URL=https://name.acryl.io/gms
export DATAHUB_GMS_TOKEN=<your-token>
```

### Claude Desktop

```bash
mcp install mcp_server.py
```

### Cursor

```json
# In .cursor/mcp.json
{
  "mcpServers": {
    "datahub": {
      "command": "<path>/mcp-server-datahub/.venv/bin/mcp",
      "args": [
        "run",
        "<path>/mcp-server-datahub/mcp_server.py"
      ],
      "env": {}
    }
  }
}
```

### Other MCP Clients

```yaml
command: <path>/mcp-server-datahub/.venv/bin/mcp
args:
  - run
  - <path>/mcp-server-datahub/mcp_server.py
```

## Developing

### Setup

```bash
uv sync --no-sources
# Alternatively, if also developing on acryl-datahub:
# Assumes the datahub repo is checked out at ../datahub
uv sync

# <authentication is the same as above>
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

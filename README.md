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

For authentication, you can either use `datahub init` to configure a global `~/.datahubenv` file, or you can set the appropriate environment variables:

```bash
uvx --from acryl-datahub datahub init   # follow the prompts

# Alternatively, use these environment variables:
export DATAHUB_GMS_URL=https://name.acryl.io/gms
export DATAHUB_GMS_TOKEN=<your-token>
```

### Claude Desktop

In your `claude_desktop_config.json` file, add the following:

```json
{
  "mcpServers": {
    "datahub": {
      "command": "uvx",
      "args": ["mcp-server-datahub"]
    }
  }
}
```

### Cursor

In `.cursor/mcp.json`, add the following:

```json
{
  "mcpServers": {
    "datahub": {
      "command": "uvx",
      "args": ["mcp-server-datahub"]
      "env": {}
    }
  }
}
```

### Other MCP Clients

```yaml
command: uvx
args:
  - mcp-server-datahub
```

## Developing

See [DEVELOPING.md](DEVELOPING.md).

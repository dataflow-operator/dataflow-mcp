# DataFlow MCP Server

MCP (Model Context Protocol) server for generating DataFlow manifests and migrating Kafka Connect configurations to DataFlow. Runs without access to Kubernetes or Prometheus — YAML generation and validation only.

## Features

- **generate_dataflow_manifest** — generate a DataFlow YAML manifest from a description (source/sink type, optional configs and transformations).
- **validate_dataflow_manifest** — validate a YAML manifest (apiVersion, kind, spec.source, spec.sink).
- **migrate_kafka_connect_to_dataflow** — migrate a Kafka Connect configuration (one or two connectors: source + sink) into a DataFlow manifest with notes on migration boundaries.
- **list_dataflow_connectors** — reference of supported connectors (sources and sinks).
- **list_dataflow_transformations** — reference of transformations with examples.

## Build

```bash
cargo build --release
```

Binary: `target/release/dataflow-mcp` (or `dataflow-mcp.exe` on Windows).

## Installation via Docker

You can run the server from a Docker image without installing Rust. The image is published to GitHub Container Registry on push to `main` and on tags.

**Image:** `ghcr.io/dataflow-operator/dataflow-mcp`

### Running the image

```bash
docker run -i --rm ghcr.io/dataflow-operator/dataflow-mcp:latest
```

The server communicates over stdin/stdout, so the `-i` flag is required. For Cursor, add this to your MCP settings:

```json
{
  "mcpServers": {
    "dataflow": {
      "command": "docker",
      "args": [
        "run",
        "-i",
        "--rm",
        "ghcr.io/dataflow-operator/dataflow-mcp:latest"
      ]
    }
  }
}
```

For a specific version, use a tag instead of `latest` (e.g. `main` or a commit SHA).

### Building the image locally

From the repository root:

```bash
docker build -t dataflow-mcp:local .
docker run -i --rm dataflow-mcp:local
```

## Connecting in Cursor

Add the server to MCP settings (e.g. `~/.cursor/mcp.json` or project settings):

```json
{
  "mcpServers": {
    "dataflow": {
      "command": "/absolute/path/to/dataflow-mcp/target/release/dataflow-mcp",
      "args": []
    }
  }
}
```

Or via `cargo run` for development:

```json
{
  "mcpServers": {
    "dataflow": {
      "command": "cargo",
      "args": ["run", "--release", "--manifest-path", "/path/to/dataflow-mcp/Cargo.toml"]
    }
  }
}
```

The server communicates over stdin/stdout (stdio).

## Testing with MCP Inspector

[MCP Inspector](https://modelcontextprotocol.io/docs/tools/inspector) is an interactive browser-based tool for testing and debugging MCP servers (like Postman for MCP). Useful for calling tools manually and inspecting responses without Cursor.

### Running

No installation required; Node.js and `npx` are enough:

```bash
npx @modelcontextprotocol/inspector /absolute/path/to/dataflow-mcp/target/release/dataflow-mcp
```

Or from the repo root after `cargo build --release`:

```bash
npx @modelcontextprotocol/inspector "$(pwd)/target/release/dataflow-mcp"
```

The web UI opens at <http://localhost:6274>. In the connection pane, select **stdio** transport and optionally set the binary path and arguments.

### What to check

- **Tools** — list of tools (`generate_dataflow_manifest`, `validate_dataflow_manifest`, `migrate_kafka_connect_to_dataflow`, `list_dataflow_connectors`, `list_dataflow_transformations`), call with JSON parameters and view raw responses.
- **Notifications** — server logs and notifications.

Recommended workflow: edit code → `cargo build --release` → click Reconnect in Inspector → re-run tool calls.

## Examples

### Generating a Kafka → PostgreSQL manifest

Use the **generate_dataflow_manifest** tool with:

- `source_type`: `"kafka"`
- `sink_type`: `"postgresql"`
- `source_config`: `"{\"brokers\":[\"localhost:9092\"],\"topic\":\"input-topic\",\"consumerGroup\":\"dataflow-group\"}"`
- `sink_config`: `"{\"connectionString\":\"postgres://user:pass@host:5432/db\",\"table\":\"output_table\"}"`
- `name`: `"kafka-to-postgres"` (optional)

### Migrating a Kafka Connect JDBC Sink

Use **migrate_kafka_connect_to_dataflow** with the connector config as JSON:

```json
{
  "name": "jdbc-sink",
  "config": {
    "connector.class": "io.confluent.connect.jdbc.JdbcSinkConnector",
    "connection.url": "jdbc:postgresql://pg:5432/mydb",
    "table.name.format": "events",
    "topics": "events"
  }
}
```

The response will include a DataFlow YAML manifest and notes on migrated and unsupported options.

### Validating a manifest

Paste the YAML manifest into **validate_dataflow_manifest** (parameter `config`). The response will indicate whether the config is valid or list errors.

## Tests

```bash
cargo test
```

## Differences from Go MCP (dataflow/mcp-dataflow)

- **No Kubernetes/Prometheus** — this server does not talk to the cluster or fetch metrics.
- **Focus** — manifest generation and Kafka Connect migration; useful in the IDE for “write a manifest” and “migrate connector to DataFlow” workflows.
- For full DataFlow management in the cluster (CRUD, metrics), use the [Go MCP server](https://github.com/dataflow-operator/dataflow-operator/tree/main/dataflow/mcp-dataflow) from the DataFlow operator repository.

// Reference data for connectors and transformations (list_dataflow_connectors, list_dataflow_transformations).

use super::common::pretty_json_from_raw;

pub fn list_dataflow_connectors_json() -> String {
    pretty_json_from_raw(default_connectors_raw())
}

fn default_connectors_raw() -> &'static str {
    r#"{
  "sources": {
    "kafka": {
      "description": "Read messages from Kafka topics",
      "required_fields": ["brokers", "topic"],
      "optional_fields": ["consumerGroup", "tls", "sasl", "format", "avroSchema", "schemaRegistry"]
    },
    "postgresql": {
      "description": "Read from PostgreSQL tables",
      "required_fields": ["connectionString", "table"],
      "optional_fields": ["query", "pollInterval"]
    },
    "trino": {
      "description": "Read from Trino tables",
      "required_fields": ["serverURL", "catalog", "schema", "table"],
      "optional_fields": ["query", "pollInterval", "keycloak"]
    },
    "clickhouse": {
      "description": "Read from ClickHouse tables",
      "required_fields": ["connectionString", "table"],
      "optional_fields": ["query", "pollInterval"]
    }
  },
  "sinks": {
    "kafka": {
      "description": "Write messages to Kafka topics",
      "required_fields": ["brokers", "topic"],
      "optional_fields": ["tls", "sasl"]
    },
    "postgresql": {
      "description": "Write to PostgreSQL tables",
      "required_fields": ["connectionString", "table"],
      "optional_fields": ["batchSize", "batchFlushIntervalSeconds", "autoCreateTable", "upsertMode", "conflictKey", "rawMode"]
    },
    "trino": {
      "description": "Write to Trino tables",
      "required_fields": ["serverURL", "catalog", "schema", "table"],
      "optional_fields": ["batchSize", "batchFlushIntervalSeconds", "autoCreateTable", "rawMode", "keycloak"]
    },
    "clickhouse": {
      "description": "Write to ClickHouse tables",
      "required_fields": ["connectionString", "table"],
      "optional_fields": ["batchSize", "batchFlushIntervalSeconds", "autoCreateTable", "rawMode"]
    }
  }
}"#
}

pub fn list_dataflow_transformations_json() -> String {
    pretty_json_from_raw(default_transformations_raw())
}

fn default_transformations_raw() -> &'static str {
    r#"{
  "timestamp": {
    "description": "Add timestamp to each message",
    "example": { "type": "timestamp", "timestamp": { "fieldName": "created_at", "format": "RFC3339" } }
  },
  "flatten": {
    "description": "Flatten array into separate messages",
    "example": { "type": "flatten", "flatten": { "field": "$.items" } }
  },
  "filter": {
    "description": "Filter messages by JSONPath condition",
    "example": { "type": "filter", "filter": { "condition": "$.level != 'error'" } }
  },
  "mask": {
    "description": "Mask sensitive fields",
    "example": { "type": "mask", "mask": { "fields": ["$.password", "$.token"], "maskChar": "*", "keepLength": true } }
  },
  "router": {
    "description": "Route messages to different sinks by condition",
    "example": { "type": "router", "router": { "routes": [{ "condition": "$.level == 'error'", "sink": { "type": "kafka", "kafka": { "brokers": ["localhost:9092"], "topic": "errors" } } }] } }
  },
  "select": {
    "description": "Select specific fields",
    "example": { "type": "select", "select": { "fields": ["$.id", "$.name", "$.timestamp"] } }
  },
  "remove": {
    "description": "Remove specific fields",
    "example": { "type": "remove", "remove": { "fields": ["$.password", "$.token"] } }
  },
  "snakeCase": {
    "description": "Convert field names to snake_case",
    "example": { "type": "snakeCase", "snakeCase": { "deep": true } }
  },
  "camelCase": {
    "description": "Convert field names to CamelCase",
    "example": { "type": "camelCase", "camelCase": { "deep": true } }
  }
}"#
}

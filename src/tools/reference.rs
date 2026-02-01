// Reference data for connectors and transformations (list_dataflow_connectors, list_dataflow_transformations).

use serde_json::{Map as JsonMap, Value};

pub fn list_dataflow_connectors_json() -> String {
    let connectors: JsonMap<String, Value> = default_connectors();
    serde_json::to_string_pretty(&connectors).unwrap_or_else(|_| default_connectors_raw().to_string())
}

fn default_connectors() -> JsonMap<String, Value> {
    serde_json::from_str(default_connectors_raw()).unwrap_or_default()
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
      "optional_fields": ["batchSize", "autoCreateTable", "upsertMode", "conflictKey"]
    },
    "trino": {
      "description": "Write to Trino tables",
      "required_fields": ["serverURL", "catalog", "schema", "table"],
      "optional_fields": ["batchSize", "autoCreateTable", "keycloak"]
    }
  }
}"#
}

pub fn list_dataflow_transformations_json() -> String {
    let transformations: JsonMap<String, Value> = default_transformations();
    serde_json::to_string_pretty(&transformations).unwrap_or_else(|_| default_transformations_raw().to_string())
}

fn default_transformations() -> JsonMap<String, Value> {
    serde_json::from_str(default_transformations_raw()).unwrap_or_default()
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

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
      "optional_fields": ["consumerGroup", "securityProtocol", "tls", "sasl", "format", "avroSchema", "schemaRegistry"]
    },
    "postgresql": {
      "description": "Read from PostgreSQL tables",
      "required_fields": ["connectionString", "table"],
      "optional_fields": ["query", "pollInterval"]
    },
    "postgresql-cdc": {
      "description": "Read from PostgreSQL via logical replication (pgoutput)",
      "required_fields": ["connectionString", "slotName", "publicationName", "tables"],
      "optional_fields": ["snapshotMode", "createSlotIfNotExists", "createPublicationIfNotExists", "heartbeatIntervalSeconds", "primaryKeyColumn", "includeColumns", "excludeColumns", "envelopeFormat", "connectionStringSecretRef", "slotNameSecretRef", "publicationNameSecretRef"]
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
      "optional_fields": ["securityProtocol", "tls", "sasl"]
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
  },
  "debeziumUnwrap": {
    "description": "Unwrap Debezium envelope; optional __op/__deleted and source_* payload fields",
    "example": { "type": "debeziumUnwrap", "debeziumUnwrap": { "inferDeleteFromTombstone": true, "includeSourceInMetadata": true, "snapshotOperation": "insert", "addOperationFields": true, "addSourceFields": ["table", "lsn", "ts_ms"] } }
  },
  "replaceField": {
    "description": "Rename fields and optionally include/exclude without flattening",
    "example": { "type": "replaceField", "replaceField": { "renames": ["oldName:newName", "key.sku:sku"], "include": ["id", "name"] } }
  },
  "headersToPayload": {
    "description": "Copy Kafka/message headers from Metadata into JSON payload fields",
    "example": { "type": "headersToPayload", "headersToPayload": { "mappings": ["X-Request-Id:requestId", "X-Language:metadata.language"] } }
  },
  "structFlatten": {
    "description": "Flatten nested JSON objects into a single-level map (arrays preserved as values)",
    "example": { "type": "structFlatten", "structFlatten": { "delimiter": "." } }
  },
  "extractField": {
    "description": "Replace the payload with the value of one field (ExtractField$Value style)",
    "example": { "type": "extractField", "extractField": { "field": "payload.after" } }
  },
  "hoistField": {
    "description": "Wrap the entire payload under a single top-level key (inverse of extractField)",
    "example": { "type": "hoistField", "hoistField": { "field": "record" } }
  },
  "cast": {
    "description": "Cast field values to target types (string/int64/float64/bool/null); failed conversion skips the message",
    "example": { "type": "cast", "cast": { "spec": { "id": "int64", "amount": "float64", "active": "bool", "note": "string", "deleted_at": "null" } } }
  },
  "timezone": {
    "description": "Convert temporal fields to a target IANA timezone or ±HH:MM offset (RFC3339/epoch in, RFC3339Nano/RFC3339/UnixMilli out)",
    "example": { "type": "timezone", "timezone": { "timezone": "Europe/Moscow", "fields": ["created_at", "updated_at"], "sourceTimezone": "UTC", "format": "RFC3339" } }
  },
  "insertField": {
    "description": "Insert or overwrite JSON fields with literals, ${metadata.*}, ${now}, or json:<raw> (Connect InsertField style)",
    "example": { "type": "insertField", "insertField": { "fields": { "pipeline": "orders-cdc", "source_topic": "${metadata.topic}", "ingested_at": "${now}", "flags.reprocessed": "json:false" } } }
  }
}"#
}

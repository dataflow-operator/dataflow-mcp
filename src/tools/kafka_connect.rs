// Migrate Kafka Connect connector config(s) to DataFlow manifest.

use super::common::{build_dataflow_toplevel, to_yaml_string};
use crate::error::{DataFlowError, Result};
use serde_json::{Map as JsonMap, Value};
use std::collections::HashMap;

/// Kafka Connect connector config: name + config map (from REST API).
#[derive(Debug, Clone, serde::Deserialize)]
pub struct KafkaConnectConnector {
    pub name: Option<String>,
    pub config: Option<HashMap<String, String>>,
}

/// One connector or array of two (source, sink).
fn parse_input(json: &str) -> Result<Vec<KafkaConnectConnector>> {
    let v: Value = serde_json::from_str(json).map_err(|e| DataFlowError::Json {
        context: "kafka_connect_config".to_string(),
        source: e,
    })?;
    if let Some(arr) = v.as_array() {
        let mut out = Vec::new();
        for item in arr {
            let c: KafkaConnectConnector = serde_json::from_value(item.clone())
                .map_err(|e| DataFlowError::Json {
                    context: "connector item".to_string(),
                    source: e,
                })?;
            out.push(c);
        }
        Ok(out)
    } else {
        let c: KafkaConnectConnector = serde_json::from_value(v)
            .map_err(|e| DataFlowError::Json {
                context: "connector".to_string(),
                source: e,
            })?;
        Ok(vec![c])
    }
}

fn get(config: &HashMap<String, String>, key: &str) -> Option<String> {
    config.get(key).cloned().or_else(|| {
        let key_lower = key.to_lowercase();
        config.iter().find(|(k, _)| k.to_lowercase() == key_lower).map(|(_, v)| v.clone())
    })
}

fn brokers_from_bootstrap_servers(s: &str) -> Vec<String> {
    s.split(',').map(|x| x.trim().to_string()).filter(|x| !x.is_empty()).collect::<Vec<_>>()
}

/// Detects connector direction and type from connector.class.
fn connector_kind(connector_class: &str) -> (&'static str, &'static str) {
    let c = connector_class.to_lowercase();
    if c.contains("debezium") || c.contains("mysql") && c.contains("cdc") {
        return ("unsupported", "debezium");
    }
    if (c.contains("jdbc") || c.contains("postgres")) && c.contains("sink") {
        return ("sink", "postgresql");
    }
    if c.contains("sink") && c.contains("kafka") {
        return ("sink", "kafka");
    }
    if c.contains("source") && c.contains("kafka") {
        return ("source", "kafka");
    }
    if c.contains("source") {
        return ("source", "kafka");
    }
    if c.contains("sink") {
        return ("sink", "kafka");
    }
    ("unknown", "unknown")
}

/// Builds a DataFlow kafka connector spec from Kafka Connect config.
/// `default_topic` is used when topics/topic keys are missing.
/// `include_consumer_group` adds consumerGroup and schema registry handling (source-only).
fn build_kafka_config(
    config: &HashMap<String, String>,
    default_topic: &str,
    include_consumer_group: bool,
) -> (JsonMap<String, Value>, Vec<String>) {
    let notes = Vec::new();
    let brokers = get(config, "bootstrap.servers")
        .map(|s| brokers_from_bootstrap_servers(&s))
        .unwrap_or_default();
    let topic = get(config, "topics")
        .or_else(|| get(config, "topic"))
        .unwrap_or_else(|| default_topic.to_string());

    let mut config_obj: JsonMap<String, Value> = JsonMap::new();
    config_obj.insert(
        "brokers".to_string(),
        Value::Array(brokers.into_iter().map(Value::String).collect()),
    );
    config_obj.insert("topic".to_string(), Value::String(topic));

    if include_consumer_group {
        if let Some(cg) = get(config, "group.id").or_else(|| get(config, "consumer.group")) {
            config_obj.insert("consumerGroup".to_string(), Value::String(cg));
        }
        if get(config, "value.converter").as_deref()
            == Some("io.confluent.connect.avro.AvroConverter")
        {
            if let Some(url) = get(config, "schema.registry.url") {
                let mut sr: JsonMap<String, Value> = JsonMap::new();
                sr.insert("url".to_string(), Value::String(url));
                config_obj.insert("schemaRegistry".to_string(), Value::Object(sr));
                config_obj.insert("format".to_string(), Value::String("avro".to_string()));
            }
        }
    }

    let mut connector: JsonMap<String, Value> = JsonMap::new();
    connector.insert("type".to_string(), Value::String("kafka".to_string()));
    connector.insert("config".to_string(), Value::Object(config_obj));
    (connector, notes)
}

/// Builds a default kafka connector spec with localhost broker and the given topic.
fn default_kafka_connector(topic: &str) -> JsonMap<String, Value> {
    let mut config: JsonMap<String, Value> = JsonMap::new();
    config.insert(
        "brokers".to_string(),
        Value::Array(vec![Value::String("localhost:9092".to_string())]),
    );
    config.insert("topic".to_string(), Value::String(topic.to_string()));

    let mut connector: JsonMap<String, Value> = JsonMap::new();
    connector.insert("type".to_string(), Value::String("kafka".to_string()));
    connector.insert("config".to_string(), Value::Object(config));
    connector
}

/// Builds DataFlow sink spec (postgresql) from JDBC Sink config.
fn map_jdbc_sink(config: &HashMap<String, String>) -> (JsonMap<String, Value>, Vec<String>) {
    let mut notes = Vec::new();
    let connection_string = get(config, "connection.url").unwrap_or_else(|| "postgres://user:pass@localhost:5432/db".to_string());
    let table = get(config, "table.name.format")
        .or_else(|| get(config, "topics"))
        .unwrap_or_else(|| "output_table".to_string());
    if get(config, "table.name.format").is_none() && get(config, "topics").is_some() {
        notes.push("Table name derived from topics; consider setting table.name.format in Kafka Connect or adjust in DataFlow.".to_string());
    }

    let mut config_obj: JsonMap<String, Value> = JsonMap::new();
    config_obj.insert("connectionString".to_string(), Value::String(connection_string));
    config_obj.insert("table".to_string(), Value::String(table));

    let mut sink: JsonMap<String, Value> = JsonMap::new();
    sink.insert("type".to_string(), Value::String("postgresql".to_string()));
    sink.insert("config".to_string(), Value::Object(config_obj));
    (sink, notes)
}

/// Migrates Kafka Connect config JSON to DataFlow YAML manifest + migration notes.
pub fn migrate_kafka_connect_to_dataflow(kafka_connect_config: &str) -> Result<String> {
    let connectors = parse_input(kafka_connect_config)?;
    let mut all_notes: Vec<String> = Vec::new();

    let mut source_spec: Option<JsonMap<String, Value>> = None;
    let mut sink_spec: Option<JsonMap<String, Value>> = None;

    for conn in &connectors {
        let config = conn.config.as_ref().ok_or_else(|| {
            DataFlowError::Other("Each connector must have 'config'".to_string())
        })?;
        let connector_class = get(config, "connector.class").unwrap_or_else(|| "unknown".to_string());
        let (direction, kind) = connector_kind(&connector_class);

        if direction == "unsupported" || kind == "debezium" {
            all_notes.push(format!(
                "Connector '{}' (class: {}) is not auto-mapped. For CDC (e.g. Debezium), use Kafka as source in DataFlow if the output is already in a Kafka topic.",
                conn.name.as_deref().unwrap_or("?"),
                connector_class
            ));
            continue;
        }
        if direction == "unknown" {
            all_notes.push(format!(
                "Unknown connector class '{}'; manual migration required.",
                connector_class
            ));
            continue;
        }

        if direction == "source" && kind == "kafka" {
            let (spec, notes) = build_kafka_config(config, "input-topic", true);
            source_spec = Some(spec);
            all_notes.extend(notes);
        } else if direction == "sink" && kind == "kafka" {
            let (spec, notes) = build_kafka_config(config, "output-topic", false);
            sink_spec = Some(spec);
            all_notes.extend(notes);
        } else if direction == "sink" && kind == "postgresql" {
            let (spec, notes) = map_jdbc_sink(config);
            sink_spec = Some(spec);
            all_notes.extend(notes);
        }
    }

    let name = connectors
        .first()
        .and_then(|c| c.name.as_ref())
        .map(|s| s.to_string())
        .unwrap_or_else(|| "dataflow-from-connect".to_string());

    let mut metadata: JsonMap<String, Value> = JsonMap::new();
    metadata.insert("name".to_string(), Value::String(sanitize_name(&name)));

    let mut spec: JsonMap<String, Value> = JsonMap::new();
    if let Some(s) = source_spec {
        spec.insert("source".to_string(), Value::Object(s));
    } else {
        all_notes.push("No supported source connector found; add source block manually (e.g. kafka).".to_string());
        spec.insert("source".to_string(), Value::Object(default_kafka_connector("input-topic")));
    }
    if let Some(s) = sink_spec {
        spec.insert("sink".to_string(), Value::Object(s));
    } else {
        all_notes.push("No supported sink connector found; add sink block manually (e.g. kafka or postgresql).".to_string());
        spec.insert("sink".to_string(), Value::Object(default_kafka_connector("output-topic")));
    }

    let top = build_dataflow_toplevel(metadata, spec);
    let yaml = to_yaml_string(&top)?;
    let mut out = String::from("# DataFlow manifest generated from Kafka Connect config\n");
    if !all_notes.is_empty() {
        out.push_str("# Migration notes:\n");
        for n in &all_notes {
            out.push_str(&format!("# - {}\n", n));
        }
    }
    out.push_str("\n");
    out.push_str(&yaml);
    Ok(out)
}

fn sanitize_name(s: &str) -> String {
    s.chars()
        .map(|c| if c.is_alphanumeric() || c == '-' { c } else { '-' })
        .collect::<String>()
        .trim_matches('-')
        .to_string()
        .to_lowercase()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_migrate_jdbc_sink_to_postgresql() {
        let config = r#"{
            "name": "jdbc-sink",
            "config": {
                "connector.class": "io.confluent.connect.jdbc.JdbcSinkConnector",
                "connection.url": "jdbc:postgresql://pg:5432/mydb",
                "table.name.format": "events",
                "topics": "events"
            }
        }"#;
        let out = migrate_kafka_connect_to_dataflow(config).unwrap();
        assert!(out.contains("apiVersion: dataflow.dataflow.io/v1"));
        assert!(out.contains("kind: DataFlow"));
        assert!(out.contains("config:"));
        assert!(out.contains("connectionString:"));
        assert!(out.contains("jdbc:postgresql"));
        assert!(out.contains("events"));
    }

    #[test]
    fn test_migrate_kafka_source() {
        let config = r#"{
            "name": "kafka-source",
            "config": {
                "connector.class": "org.apache.kafka.connect.source.SomeKafkaSource",
                "bootstrap.servers": "broker1:9092,broker2:9092",
                "topics": "input-topic",
                "group.id": "my-group"
            }
        }"#;
        let out = migrate_kafka_connect_to_dataflow(config).unwrap();
        assert!(out.contains("source:"));
        assert!(out.contains("config:"));
        assert!(out.contains("brokers:"));
        assert!(out.contains("broker1:9092"));
        assert!(out.contains("input-topic"));
        assert!(out.contains("consumerGroup:") || out.contains("my-group"));
    }

    #[test]
    fn test_migrate_unknown_connector_has_manual_note() {
        let config = r#"{
            "name": "unknown",
            "config": {
                "connector.class": "com.example.UnknownConnector"
            }
        }"#;
        let out = migrate_kafka_connect_to_dataflow(config).unwrap();
        assert!(out.contains("Unknown connector") || out.contains("manual") || out.contains("DataFlow"));
        assert!(out.contains("apiVersion: dataflow.dataflow.io/v1"));
    }
}

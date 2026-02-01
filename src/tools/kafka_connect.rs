// Migrate Kafka Connect connector config(s) to DataFlow manifest.

use crate::types::{DATAFLOW_API_VERSION, DATAFLOW_KIND};
use serde_json::{Map as JsonMap, Value};
use std::collections::HashMap;

/// Kafka Connect connector config: name + config map (from REST API).
#[derive(Debug, Clone, serde::Deserialize)]
pub struct KafkaConnectConnector {
    pub name: Option<String>,
    pub config: Option<HashMap<String, String>>,
}

/// One connector or array of two (source, sink).
fn parse_input(json: &str) -> Result<Vec<KafkaConnectConnector>, String> {
    let v: Value = serde_json::from_str(json).map_err(|e| format!("Invalid JSON: {}", e))?;
    if let Some(arr) = v.as_array() {
        let mut out = Vec::new();
        for item in arr {
            let c: KafkaConnectConnector = serde_json::from_value(item.clone())
                .map_err(|e| format!("Connector item invalid: {}", e))?;
            out.push(c);
        }
        Ok(out)
    } else {
        let c: KafkaConnectConnector = serde_json::from_value(v).map_err(|e| format!("Invalid connector: {}", e))?;
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

/// Builds DataFlow source spec (kafka) from Kafka Connect source config.
fn map_kafka_source(config: &HashMap<String, String>) -> (JsonMap<String, Value>, Vec<String>) {
    let notes = Vec::new();
    let brokers = get(config, "bootstrap.servers")
        .map(|s| brokers_from_bootstrap_servers(&s))
        .unwrap_or_default();
    let topic = get(config, "topics")
        .or_else(|| get(config, "topic"))
        .unwrap_or_else(|| "input-topic".to_string());
    let consumer_group = get(config, "group.id").or_else(|| get(config, "consumer.group"));

    let mut kafka: JsonMap<String, Value> = JsonMap::new();
    kafka.insert("brokers".to_string(), Value::Array(brokers.into_iter().map(Value::String).collect()));
    kafka.insert("topic".to_string(), Value::String(topic));
    if let Some(cg) = consumer_group {
        kafka.insert("consumerGroup".to_string(), Value::String(cg));
    }
    if get(config, "value.converter").as_deref() == Some("io.confluent.connect.avro.AvroConverter") {
        if let Some(url) = get(config, "schema.registry.url") {
            let mut sr: JsonMap<String, Value> = JsonMap::new();
            sr.insert("url".to_string(), Value::String(url));
            kafka.insert("schemaRegistry".to_string(), Value::Object(sr));
            kafka.insert("format".to_string(), Value::String("avro".to_string()));
        }
    }

    let mut source: JsonMap<String, Value> = JsonMap::new();
    source.insert("type".to_string(), Value::String("kafka".to_string()));
    source.insert("kafka".to_string(), Value::Object(kafka));
    (source, notes)
}

/// Builds DataFlow sink spec (kafka) from Kafka Connect sink config.
fn map_kafka_sink(config: &HashMap<String, String>) -> (JsonMap<String, Value>, Vec<String>) {
    let notes = Vec::new();
    let brokers = get(config, "bootstrap.servers")
        .map(|s| brokers_from_bootstrap_servers(&s))
        .unwrap_or_default();
    let topic = get(config, "topics")
        .or_else(|| get(config, "topic"))
        .unwrap_or_else(|| "output-topic".to_string());

    let mut kafka: JsonMap<String, Value> = JsonMap::new();
    kafka.insert("brokers".to_string(), Value::Array(brokers.into_iter().map(Value::String).collect()));
    kafka.insert("topic".to_string(), Value::String(topic));

    let mut sink: JsonMap<String, Value> = JsonMap::new();
    sink.insert("type".to_string(), Value::String("kafka".to_string()));
    sink.insert("kafka".to_string(), Value::Object(kafka));
    (sink, notes)
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

    let mut postgresql: JsonMap<String, Value> = JsonMap::new();
    postgresql.insert("connectionString".to_string(), Value::String(connection_string));
    postgresql.insert("table".to_string(), Value::String(table));

    let mut sink: JsonMap<String, Value> = JsonMap::new();
    sink.insert("type".to_string(), Value::String("postgresql".to_string()));
    sink.insert("postgresql".to_string(), Value::Object(postgresql));
    (sink, notes)
}

/// Migrates Kafka Connect config JSON to DataFlow YAML manifest + migration notes.
pub fn migrate_kafka_connect_to_dataflow(kafka_connect_config: &str) -> Result<String, String> {
    let connectors = parse_input(kafka_connect_config)?;
    let mut all_notes: Vec<String> = Vec::new();

    let mut source_spec: Option<JsonMap<String, Value>> = None;
    let mut sink_spec: Option<JsonMap<String, Value>> = None;

    for conn in &connectors {
        let config = conn.config.as_ref().ok_or("Each connector must have 'config'")?;
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
            let (spec, notes) = map_kafka_source(config);
            source_spec = Some(spec);
            all_notes.extend(notes);
        } else if direction == "sink" && kind == "kafka" {
            let (spec, notes) = map_kafka_sink(config);
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
        let mut default_source: JsonMap<String, Value> = JsonMap::new();
        default_source.insert("type".to_string(), Value::String("kafka".to_string()));
        default_source.insert("kafka".to_string(), Value::Object({
            let mut k: JsonMap<String, Value> = JsonMap::new();
            k.insert("brokers".to_string(), Value::Array(vec![Value::String("localhost:9092".to_string())]));
            k.insert("topic".to_string(), Value::String("input-topic".to_string()));
            k
        }));
        spec.insert("source".to_string(), Value::Object(default_source));
    }
    if let Some(s) = sink_spec {
        spec.insert("sink".to_string(), Value::Object(s));
    } else {
        all_notes.push("No supported sink connector found; add sink block manually (e.g. kafka or postgresql).".to_string());
        let mut default_sink: JsonMap<String, Value> = JsonMap::new();
        default_sink.insert("type".to_string(), Value::String("kafka".to_string()));
        default_sink.insert("kafka".to_string(), Value::Object({
            let mut k: JsonMap<String, Value> = JsonMap::new();
            k.insert("brokers".to_string(), Value::Array(vec![Value::String("localhost:9092".to_string())]));
            k.insert("topic".to_string(), Value::String("output-topic".to_string()));
            k
        }));
        spec.insert("sink".to_string(), Value::Object(default_sink));
    }

    let mut top: JsonMap<String, Value> = JsonMap::new();
    top.insert("apiVersion".to_string(), Value::String(DATAFLOW_API_VERSION.to_string()));
    top.insert("kind".to_string(), Value::String(DATAFLOW_KIND.to_string()));
    top.insert("metadata".to_string(), Value::Object(metadata));
    top.insert("spec".to_string(), Value::Object(spec));

    let yaml = serde_yaml::to_string(&top).map_err(|e| e.to_string())?;
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
        assert!(out.contains("postgresql:"));
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
        assert!(out.contains("kafka:"));
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

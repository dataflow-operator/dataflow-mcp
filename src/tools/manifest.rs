// Generate and validate DataFlow manifests.

use crate::types::{ParsedDataFlow, DATAFLOW_API_VERSION, DATAFLOW_KIND, SINK_TYPES, SOURCE_TYPES};
use serde_json::{Map as JsonMap, Value};

/// Generates a DataFlow YAML manifest from the given parameters.
/// source_config and sink_config are optional JSON objects (as strings); if provided they are merged under source[source_type] and sink[sink_type].
/// transformations is optional JSON array string.
pub fn generate_dataflow_manifest(
    description: Option<&str>,
    source_type: &str,
    sink_type: &str,
    source_config: Option<&str>,
    sink_config: Option<&str>,
    transformations: Option<&str>,
    name: Option<&str>,
    namespace: Option<&str>,
) -> Result<String, String> {
    if !SOURCE_TYPES.contains(&source_type) {
        return Err(format!(
            "source_type must be one of: {}",
            SOURCE_TYPES.join(", ")
        ));
    }
    if !SINK_TYPES.contains(&sink_type) {
        return Err(format!(
            "sink_type must be one of: {}",
            SINK_TYPES.join(", ")
        ));
    }

    let mut metadata: JsonMap<String, Value> = JsonMap::new();
    metadata.insert(
        "name".to_string(),
        Value::String(name.unwrap_or("dataflow-example").to_string()),
    );
    if let Some(ns) = namespace {
        metadata.insert("namespace".to_string(), Value::String(ns.to_string()));
    }

    let mut source: JsonMap<String, Value> = JsonMap::new();
    source.insert("type".to_string(), Value::String(source_type.to_string()));
    let source_config_obj: JsonMap<String, Value> = if let Some(sc) = source_config {
        serde_json::from_str(sc).map_err(|e| format!("source_config invalid JSON: {}", e))?
    } else {
        JsonMap::new()
    };
    source.insert(source_type.to_string(), Value::Object(source_config_obj));

    let mut sink: JsonMap<String, Value> = JsonMap::new();
    sink.insert("type".to_string(), Value::String(sink_type.to_string()));
    let sink_config_obj: JsonMap<String, Value> = if let Some(sc) = sink_config {
        serde_json::from_str(sc).unwrap_or_else(|_| JsonMap::new())
    } else {
        JsonMap::new()
    };
    sink.insert(sink_type.to_string(), Value::Object(sink_config_obj));

    let mut spec: JsonMap<String, Value> = JsonMap::new();
    spec.insert("source".to_string(), Value::Object(source));
    spec.insert("sink".to_string(), Value::Object(sink));
    if let Some(tr) = transformations {
        let arr: Value = serde_json::from_str(tr).map_err(|e| format!("transformations invalid JSON: {}", e))?;
        if let Value::Array(a) = arr {
            if !a.is_empty() {
                spec.insert("transformations".to_string(), Value::Array(a));
            }
        }
    }

    let mut top: JsonMap<String, Value> = JsonMap::new();
    top.insert("apiVersion".to_string(), Value::String(DATAFLOW_API_VERSION.to_string()));
    top.insert("kind".to_string(), Value::String(DATAFLOW_KIND.to_string()));
    top.insert("metadata".to_string(), Value::Object(metadata));
    top.insert("spec".to_string(), Value::Object(spec));

    let yaml = serde_yaml::to_string(&top).map_err(|e| e.to_string())?;
    let mut out = format!("# Generated DataFlow manifest\n");
    if let Some(d) = description {
        out.push_str(&format!("# Description: {}\n", d));
    }
    out.push_str(&yaml);
    Ok(out)
}

/// Validates a DataFlow YAML manifest: parsing, apiVersion/kind, spec.source/spec.sink, and basic required fields per type.
pub fn validate_dataflow_manifest(config_yaml: &str) -> Result<(), Vec<String>> {
    let parsed: ParsedDataFlow = serde_yaml::from_str(config_yaml).map_err(|e| {
        vec![format!("YAML parse error: {}", e)]
    })?;

    let mut errors = Vec::new();

    if parsed.api_version.as_deref() != Some(DATAFLOW_API_VERSION) {
        errors.push(format!(
            "apiVersion must be '{}'",
            DATAFLOW_API_VERSION
        ));
    }
    if parsed.kind.as_deref() != Some(DATAFLOW_KIND) {
        errors.push(format!("kind must be '{}'", DATAFLOW_KIND));
    }
    let spec = match &parsed.spec {
        Some(s) => s,
        None => {
            errors.push("spec is required".to_string());
            return Err(errors);
        }
    };
    let source = match &spec.source {
        Some(s) => s,
        None => {
            errors.push("spec.source is required".to_string());
            return Err(errors);
        }
    };
    let sink = match &spec.sink {
        Some(s) => s,
        None => {
            errors.push("spec.sink is required".to_string());
            return Err(errors);
        }
    };

    let source_type = source.type_.as_deref().unwrap_or("");
    if !SOURCE_TYPES.contains(&source_type) {
        errors.push(format!(
            "spec.source.type must be one of: {}",
            SOURCE_TYPES.join(", ")
        ));
    } else {
        match source_type {
            "kafka" => {
                if source.kafka.is_none() {
                    errors.push("spec.source.kafka is required when source.type is kafka".to_string());
                }
            }
            "postgresql" => {
                if source.postgresql.is_none() {
                    errors.push("spec.source.postgresql is required when source.type is postgresql".to_string());
                }
            }
            "trino" => {
                if source.trino.is_none() {
                    errors.push("spec.source.trino is required when source.type is trino".to_string());
                }
            }
            "clickhouse" => {
                if source.clickhouse.is_none() {
                    errors.push("spec.source.clickhouse is required when source.type is clickhouse".to_string());
                }
            }
            _ => {}
        }
    }

    let sink_type = sink.type_.as_deref().unwrap_or("");
    if !SINK_TYPES.contains(&sink_type) {
        errors.push(format!(
            "spec.sink.type must be one of: {}",
            SINK_TYPES.join(", ")
        ));
    } else {
        match sink_type {
            "kafka" => {
                if sink.kafka.is_none() {
                    errors.push("spec.sink.kafka is required when sink.type is kafka".to_string());
                }
            }
            "postgresql" => {
                if sink.postgresql.is_none() {
                    errors.push("spec.sink.postgresql is required when sink.type is postgresql".to_string());
                }
            }
            "trino" => {
                if sink.trino.is_none() {
                    errors.push("spec.sink.trino is required when sink.type is trino".to_string());
                }
            }
            "clickhouse" => {
                if sink.clickhouse.is_none() {
                    errors.push("spec.sink.clickhouse is required when sink.type is clickhouse".to_string());
                }
            }
            _ => {}
        }
    }

    if errors.is_empty() {
        Ok(())
    } else {
        Err(errors)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_generate_dataflow_manifest_kafka_postgresql() {
        let yaml = generate_dataflow_manifest(
            Some("Kafka to PostgreSQL"),
            "kafka",
            "postgresql",
            Some(r#"{"brokers":["localhost:9092"],"topic":"input-topic"}"#),
            Some(r#"{"connectionString":"postgres://u:p@h/db","table":"t"}"#),
            None,
            Some("my-flow"),
            None,
        )
        .unwrap();
        assert!(yaml.contains("apiVersion: dataflow.dataflow.io/v1"));
        assert!(yaml.contains("kind: DataFlow"));
        assert!(yaml.contains("name: my-flow"));
        assert!(yaml.contains("spec:"));
        assert!(yaml.contains("source:"));
        assert!(yaml.contains("sink:"));
        assert!(yaml.contains("kafka:"));
        assert!(yaml.contains("postgresql:"));
        assert!(yaml.contains("brokers:"));
        assert!(yaml.contains("connectionString:"));
    }

    #[test]
    fn test_generate_dataflow_manifest_invalid_source_type() {
        let err = generate_dataflow_manifest(
            None,
            "invalid",
            "postgresql",
            None,
            None,
            None,
            None,
            None,
        )
        .unwrap_err();
        assert!(err.contains("source_type must be one of"));
    }

    #[test]
    fn test_validate_dataflow_manifest_valid() {
        let yaml = r#"
apiVersion: dataflow.dataflow.io/v1
kind: DataFlow
metadata:
  name: test
spec:
  source:
    type: kafka
    kafka:
      brokers: ["localhost:9092"]
      topic: input
  sink:
    type: postgresql
    postgresql:
      connectionString: "postgres://localhost/db"
      table: out
"#;
        assert!(validate_dataflow_manifest(yaml).is_ok());
    }

    #[test]
    fn test_validate_dataflow_manifest_wrong_kind() {
        let yaml = r#"
apiVersion: dataflow.dataflow.io/v1
kind: WrongKind
metadata:
  name: test
spec:
  source:
    type: kafka
    kafka: {}
  sink:
    type: kafka
    kafka: {}
"#;
        let err = validate_dataflow_manifest(yaml).unwrap_err();
        assert!(err.iter().any(|e| e.contains("kind")));
    }

    #[test]
    fn test_validate_dataflow_manifest_missing_spec() {
        let yaml = r#"
apiVersion: dataflow.dataflow.io/v1
kind: DataFlow
metadata:
  name: test
"#;
        let err = validate_dataflow_manifest(yaml).unwrap_err();
        assert!(!err.is_empty());
    }
}

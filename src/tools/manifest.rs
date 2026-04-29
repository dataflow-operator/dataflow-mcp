// Generate and validate DataFlow manifests.

use super::common::{build_connector_spec, build_dataflow_toplevel, parse_json, to_yaml_string};
use crate::error::{DataFlowError, Result};
use crate::types::{ParsedDataFlow, DATAFLOW_API_VERSION, DATAFLOW_KIND};
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
) -> Result<String> {
    if source_type.trim().is_empty() {
        return Err(DataFlowError::Validation(vec![
            "source_type is required".to_string(),
        ]));
    }
    if sink_type.trim().is_empty() {
        return Err(DataFlowError::Validation(vec![
            "sink_type is required".to_string(),
        ]));
    }

    let mut metadata: JsonMap<String, Value> = JsonMap::new();
    metadata.insert(
        "name".to_string(),
        Value::String(name.unwrap_or("dataflow-example").to_string()),
    );
    if let Some(ns) = namespace {
        metadata.insert("namespace".to_string(), Value::String(ns.to_string()));
    }

    let source = build_connector_spec(source_type, source_config)?;
    let sink = build_connector_spec(sink_type, sink_config)
        .unwrap_or_else(|_| build_connector_spec(sink_type, None).unwrap());

    let mut spec: JsonMap<String, Value> = JsonMap::new();
    spec.insert("source".to_string(), Value::Object(source));
    spec.insert("sink".to_string(), Value::Object(sink));
    if let Some(tr) = transformations {
        let arr: Value = parse_json(tr, "transformations")?;
        if let Value::Array(a) = arr {
            if !a.is_empty() {
                spec.insert("transformations".to_string(), Value::Array(a));
            }
        }
    }

    let top = build_dataflow_toplevel(metadata, spec);
    let yaml = to_yaml_string(&top)?;
    let mut out = String::from("# Generated DataFlow manifest\n");
    if let Some(d) = description {
        out.push_str(&format!("# Description: {}\n", d));
    }
    out.push_str(&yaml);
    Ok(out)
}

/// Validates a DataFlow YAML manifest: parsing, apiVersion/kind, spec.source/spec.sink, and basic required fields per type.
pub fn validate_dataflow_manifest(config_yaml: &str) -> Result<()> {
    let parsed: ParsedDataFlow = serde_yaml::from_str(config_yaml)?;

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
            return Err(DataFlowError::Validation(errors));
        }
    };
    let source = match &spec.source {
        Some(s) => s,
        None => {
            errors.push("spec.source is required".to_string());
            return Err(DataFlowError::Validation(errors));
        }
    };
    let sink = match &spec.sink {
        Some(s) => s,
        None => {
            errors.push("spec.sink is required".to_string());
            return Err(DataFlowError::Validation(errors));
        }
    };

    let source_type = source.type_.as_deref().unwrap_or("");
    if source_type.is_empty() {
        errors.push("spec.source.type is required".to_string());
    } else if source.config.is_none() {
        errors.push(format!("spec.source.config is required when source.type is {}", source_type));
    }

    let sink_type = sink.type_.as_deref().unwrap_or("");
    if sink_type.is_empty() {
        errors.push("spec.sink.type is required".to_string());
    } else if sink.config.is_none() {
        errors.push(format!("spec.sink.config is required when sink.type is {}", sink_type));
    }

    if errors.is_empty() {
        Ok(())
    } else {
        Err(DataFlowError::Validation(errors))
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
        assert!(yaml.contains("config:"));
        assert!(yaml.contains("brokers:"));
        assert!(yaml.contains("connectionString:"));
    }

    #[test]
    fn test_generate_dataflow_manifest_custom_types() {
        let yaml = generate_dataflow_manifest(
            None,
            "my-custom-source",
            "my-custom-sink",
            None,
            None,
            None,
            None,
            None,
        )
        .unwrap();
        assert!(yaml.contains("type: my-custom-source"));
        assert!(yaml.contains("type: my-custom-sink"));
    }

    #[test]
    fn test_generate_dataflow_manifest_requires_source_and_sink_type() {
        let source_err = generate_dataflow_manifest(
            None,
            "",
            "postgresql",
            None,
            None,
            None,
            None,
            None,
        )
        .unwrap_err();
        assert!(source_err.to_string().contains("source_type is required"));

        let sink_err = generate_dataflow_manifest(
            None,
            "kafka",
            "",
            None,
            None,
            None,
            None,
            None,
        )
        .unwrap_err();
        assert!(sink_err.to_string().contains("sink_type is required"));
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
    config:
      brokers: ["localhost:9092"]
      topic: input
  sink:
    type: postgresql
    config:
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
    config: {}
  sink:
    type: kafka
    config: {}
"#;
        let err = validate_dataflow_manifest(yaml).unwrap_err();
        match err {
            DataFlowError::Validation(errors) => {
                assert!(errors.iter().any(|e| e.contains("kind")));
            }
            other => panic!("expected Validation, got: {}", other),
        }
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
        match err {
            DataFlowError::Validation(errors) => assert!(!errors.is_empty()),
            DataFlowError::Yaml(_) => {} // also acceptable: YAML may fail to parse spec
            other => panic!("expected Validation or Yaml, got: {}", other),
        }
    }

    #[test]
    fn test_validate_dataflow_manifest_invalid_yaml() {
        let yaml = "{{not valid yaml";
        let err = validate_dataflow_manifest(yaml).unwrap_err();
        assert!(matches!(err, DataFlowError::Yaml(_)));
    }

    #[test]
    fn test_generate_dataflow_manifest_clickhouse() {
        let yaml = generate_dataflow_manifest(
            Some("Kafka to ClickHouse"),
            "kafka",
            "clickhouse",
            Some(r#"{"brokers":["localhost:9092"],"topic":"input-topic"}"#),
            Some(r#"{"connectionString":"clickhouse://default@localhost:9000/default","table":"output_table"}"#),
            None,
            Some("kafka-to-clickhouse"),
            None,
        )
        .unwrap();
        assert!(yaml.contains("apiVersion: dataflow.dataflow.io/v1"));
        assert!(yaml.contains("kind: DataFlow"));
        assert!(yaml.contains("name: kafka-to-clickhouse"));
        assert!(yaml.contains("config:"));
        assert!(yaml.contains("connectionString:"));
        assert!(yaml.contains("output_table"));
    }

    #[test]
    fn test_validate_dataflow_manifest_clickhouse() {
        let yaml = r#"
apiVersion: dataflow.dataflow.io/v1
kind: DataFlow
metadata:
  name: clickhouse-test
spec:
  source:
    type: clickhouse
    config:
      connectionString: "clickhouse://default@localhost:9000/default"
      table: source_table
  sink:
    type: clickhouse
    config:
      connectionString: "clickhouse://default@localhost:9000/default"
      table: sink_table
"#;
        assert!(validate_dataflow_manifest(yaml).is_ok());
    }

    #[test]
    fn test_validate_dataflow_manifest_custom_types() {
        let yaml = r#"
apiVersion: dataflow.dataflow.io/v1
kind: DataFlow
metadata:
  name: custom-types-test
spec:
  source:
    type: foo-source
    config:
      any: value
  sink:
    type: bar-sink
    config:
      any: value
"#;
        assert!(validate_dataflow_manifest(yaml).is_ok());
    }
}

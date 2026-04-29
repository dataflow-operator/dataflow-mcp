use crate::error::{DataFlowError, Result};
use crate::types::{DATAFLOW_API_VERSION, DATAFLOW_KIND};
use serde_json::{Map as JsonMap, Value};

/// Builds the top-level DataFlow manifest structure (apiVersion, kind, metadata, spec).
pub fn build_dataflow_toplevel(
    metadata: JsonMap<String, Value>,
    spec: JsonMap<String, Value>,
) -> JsonMap<String, Value> {
    let mut top = JsonMap::new();
    top.insert(
        "apiVersion".to_string(),
        Value::String(DATAFLOW_API_VERSION.to_string()),
    );
    top.insert(
        "kind".to_string(),
        Value::String(DATAFLOW_KIND.to_string()),
    );
    top.insert("metadata".to_string(), Value::Object(metadata));
    top.insert("spec".to_string(), Value::Object(spec));
    top
}

/// Builds a connector spec with `type` and `config` parsed from an optional JSON string.
pub fn build_connector_spec(
    connector_type: &str,
    config_json: Option<&str>,
) -> Result<JsonMap<String, Value>> {
    let config_obj: JsonMap<String, Value> = match config_json {
        Some(json) => parse_json(json, &format!("{}_config", connector_type))?,
        None => JsonMap::new(),
    };
    let mut spec = JsonMap::new();
    spec.insert(
        "type".to_string(),
        Value::String(connector_type.to_string()),
    );
    spec.insert("config".to_string(), Value::Object(config_obj));
    Ok(spec)
}

/// Serializes a value to a YAML string.
pub fn to_yaml_string(value: &impl serde::Serialize) -> Result<String> {
    Ok(serde_yaml::to_string(value)?)
}

/// Parses a JSON string with a descriptive context used in error messages.
pub fn parse_json<T: serde::de::DeserializeOwned>(s: &str, context: &str) -> Result<T> {
    serde_json::from_str(s).map_err(|e| DataFlowError::Json {
        context: context.to_string(),
        source: e,
    })
}

/// Parses raw JSON and pretty-prints it; returns the original string on parse failure.
pub fn pretty_json_from_raw(raw: &str) -> String {
    match serde_json::from_str::<Value>(raw) {
        Ok(v) => serde_json::to_string_pretty(&v).unwrap_or_else(|_| raw.to_string()),
        Err(_) => raw.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_dataflow_toplevel_sets_api_version_and_kind() {
        let mut metadata = JsonMap::new();
        metadata.insert("name".to_string(), Value::String("test".to_string()));
        let spec = JsonMap::new();
        let top = build_dataflow_toplevel(metadata, spec);
        assert_eq!(top["apiVersion"], DATAFLOW_API_VERSION);
        assert_eq!(top["kind"], DATAFLOW_KIND);
        assert_eq!(top["metadata"]["name"], "test");
        assert!(top["spec"].as_object().unwrap().is_empty());
    }

    #[test]
    fn test_build_connector_spec_with_config() {
        let spec =
            build_connector_spec("kafka", Some(r#"{"brokers":["localhost:9092"]}"#)).unwrap();
        assert_eq!(spec["type"], "kafka");
        assert!(spec["config"]["brokers"].is_array());
    }

    #[test]
    fn test_build_connector_spec_no_config() {
        let spec = build_connector_spec("postgresql", None).unwrap();
        assert_eq!(spec["type"], "postgresql");
        assert!(spec["config"].as_object().unwrap().is_empty());
    }

    #[test]
    fn test_build_connector_spec_invalid_json() {
        let err = build_connector_spec("kafka", Some("not json")).unwrap_err();
        assert!(matches!(err, DataFlowError::Json { .. }));
        let msg = err.to_string();
        assert!(msg.contains("kafka_config"));
        assert!(msg.contains("invalid JSON"));
    }

    #[test]
    fn test_to_yaml_string() {
        let mut map = JsonMap::new();
        map.insert("key".to_string(), Value::String("value".to_string()));
        let yaml = to_yaml_string(&map).unwrap();
        assert!(yaml.contains("key:"));
        assert!(yaml.contains("value"));
    }

    #[test]
    fn test_parse_json_valid() {
        let result: JsonMap<String, Value> = parse_json(r#"{"a": 1}"#, "test").unwrap();
        assert_eq!(result["a"], 1);
    }

    #[test]
    fn test_parse_json_invalid() {
        let err = parse_json::<JsonMap<String, Value>>("not json", "test_context").unwrap_err();
        assert!(matches!(err, DataFlowError::Json { .. }));
        let msg = err.to_string();
        assert!(msg.contains("test_context"));
        assert!(msg.contains("invalid JSON"));
    }

    #[test]
    fn test_pretty_json_from_raw_formats_output() {
        let raw = r#"{"a":1,"b":"hello"}"#;
        let pretty = pretty_json_from_raw(raw);
        assert!(pretty.contains("\"a\": 1"));
        assert!(pretty.contains("\"b\": \"hello\""));
    }

    #[test]
    fn test_pretty_json_from_raw_returns_original_on_invalid() {
        let raw = "not json";
        assert_eq!(pretty_json_from_raw(raw), "not json");
    }
}

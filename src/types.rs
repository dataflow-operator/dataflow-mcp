// Minimal types for DataFlow manifest validation (parsed YAML).
// Generation uses serde_json::Value maps for flexibility.

use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct ParsedDataFlow {
    #[serde(rename = "apiVersion")]
    pub api_version: Option<String>,
    pub kind: Option<String>,
    #[allow(dead_code)]
    pub metadata: Option<ParsedMetadata>,
    pub spec: Option<ParsedSpec>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ParsedMetadata {
    #[allow(dead_code)]
    pub name: Option<String>,
    #[allow(dead_code)]
    pub namespace: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ParsedSpec {
    pub source: Option<ParsedSource>,
    pub sink: Option<ParsedSink>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ParsedSource {
    #[serde(rename = "type")]
    pub type_: Option<String>,
    pub kafka: Option<serde_json::Value>,
    pub postgresql: Option<serde_json::Value>,
    pub trino: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ParsedSink {
    #[serde(rename = "type")]
    pub type_: Option<String>,
    pub kafka: Option<serde_json::Value>,
    pub postgresql: Option<serde_json::Value>,
    pub trino: Option<serde_json::Value>,
}

pub const DATAFLOW_API_VERSION: &str = "dataflow.dataflow.io/v1";
pub const DATAFLOW_KIND: &str = "DataFlow";
pub const SOURCE_TYPES: [&str; 3] = ["kafka", "postgresql", "trino"];
pub const SINK_TYPES: [&str; 3] = ["kafka", "postgresql", "trino"];

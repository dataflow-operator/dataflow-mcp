use thiserror::Error;

pub type Result<T> = std::result::Result<T, DataFlowError>;

#[derive(Debug, Error)]
pub enum DataFlowError {
    #[error("Invalid connector type '{type_name}': must be one of: {valid_types}")]
    InvalidConnectorType {
        type_name: String,
        valid_types: String,
    },

    #[error("YAML error: {0}")]
    Yaml(#[from] serde_yaml::Error),

    #[error("{context}: invalid JSON: {source}")]
    Json {
        context: String,
        source: serde_json::Error,
    },

    #[error("{}", .0.join("\n"))]
    Validation(Vec<String>),

    #[error("{0}")]
    Other(String),
}

//! DataFlow MCP server: generate manifests and migrate Kafka Connect to DataFlow.

mod tools;
mod types;

use rmcp::{
    handler::server::ServerHandler,
    model::{CallToolResult, Content},
    tool, tool_handler, tool_router,
    transport::stdio,
    ServiceExt,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
struct GenerateParams {
    /// Short description of the data flow
    #[serde(default)]
    description: Option<String>,
    /// Source type: kafka, postgresql, trino
    source_type: String,
    /// Sink type: kafka, postgresql, trino
    sink_type: String,
    /// Source config as JSON object string (optional)
    #[serde(default)]
    source_config: Option<String>,
    /// Sink config as JSON object string (optional)
    #[serde(default)]
    sink_config: Option<String>,
    /// Transformations as JSON array string (optional)
    #[serde(default)]
    transformations: Option<String>,
    /// DataFlow resource name (optional)
    #[serde(default)]
    name: Option<String>,
    /// Kubernetes namespace (optional)
    #[serde(default)]
    namespace: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
struct ValidateParams {
    /// YAML manifest to validate
    config: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
struct MigrateParams {
    /// Kafka Connect connector config(s) as JSON: single object or array of two (source, sink)
    kafka_connect_config: String,
}

#[derive(Clone)]
struct DataFlowMcpService {
    tool_router: rmcp::handler::server::tool::ToolRouter<Self>,
}

#[tool_router]
impl DataFlowMcpService {
    fn new() -> Self {
        Self {
            tool_router: Self::tool_router(),
        }
    }

    #[tool(description = "Generate a DataFlow YAML manifest from source/sink types and optional configs")]
    async fn generate_dataflow_manifest(
        &self,
        params: rmcp::handler::server::wrapper::Parameters<GenerateParams>,
    ) -> Result<CallToolResult, rmcp::ErrorData> {
        let p = params.0;
        match tools::manifest::generate_dataflow_manifest(
            p.description.as_deref(),
            &p.source_type,
            &p.sink_type,
            p.source_config.as_deref(),
            p.sink_config.as_deref(),
            p.transformations.as_deref(),
            p.name.as_deref(),
            p.namespace.as_deref(),
        ) {
            Ok(out) => Ok(CallToolResult::success(vec![Content::text(out)])),
            Err(e) => Ok(CallToolResult::error(vec![Content::text(e)])),
        }
    }

    #[tool(description = "Validate a DataFlow YAML manifest (apiVersion, kind, spec.source, spec.sink)")]
    async fn validate_dataflow_manifest(
        &self,
        params: rmcp::handler::server::wrapper::Parameters<ValidateParams>,
    ) -> Result<CallToolResult, rmcp::ErrorData> {
        let config = params.0.config;
        match tools::manifest::validate_dataflow_manifest(&config) {
            Ok(()) => Ok(CallToolResult::success(vec![Content::text("Конфигурация валидна.")])),
            Err(errors) => Ok(CallToolResult::error(vec![Content::text(format!(
                "Ошибки валидации:\n{}",
                errors.join("\n")
            ))])),
        }
    }

    #[tool(description = "Migrate Kafka Connect connector config(s) to DataFlow YAML manifest")]
    async fn migrate_kafka_connect_to_dataflow(
        &self,
        params: rmcp::handler::server::wrapper::Parameters<MigrateParams>,
    ) -> Result<CallToolResult, rmcp::ErrorData> {
        match tools::kafka_connect::migrate_kafka_connect_to_dataflow(&params.0.kafka_connect_config) {
            Ok(out) => Ok(CallToolResult::success(vec![Content::text(out)])),
            Err(e) => Ok(CallToolResult::error(vec![Content::text(e)])),
        }
    }

    #[tool(description = "List supported DataFlow connectors (sources and sinks) with fields")]
    async fn list_dataflow_connectors(&self) -> Result<CallToolResult, rmcp::ErrorData> {
        let out = tools::reference::list_dataflow_connectors_json();
        Ok(CallToolResult::success(vec![Content::text(out)]))
    }

    #[tool(description = "List DataFlow transformations with examples")]
    async fn list_dataflow_transformations(&self) -> Result<CallToolResult, rmcp::ErrorData> {
        let out = tools::reference::list_dataflow_transformations_json();
        Ok(CallToolResult::success(vec![Content::text(out)]))
    }
}

#[tool_handler]
impl ServerHandler for DataFlowMcpService {
    fn get_info(&self) -> rmcp::model::ServerInfo {
        rmcp::model::ServerInfo {
            // name: Some("dataflow".to_string()),
            // version: Some("0.1.0".to_string()),
            instructions: Some("MCP for DataFlow: generate manifests and migrate Kafka Connect to DataFlow.".to_string()),
            capabilities: rmcp::model::ServerCapabilities::builder()
                .enable_tools()
                .build(),
            ..Default::default()
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let service = DataFlowMcpService::new();
    let transport = stdio();
    let server = service.serve(transport).await?;
    server.waiting().await?;
    Ok(())
}

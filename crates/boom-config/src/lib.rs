use boom_core::GatewayError;
use serde::Deserialize;
use std::collections::HashMap;
use std::env;

/// Top-level gateway configuration, loaded from YAML.
/// Compatible with litellm's `proxy_server_config.yaml` format.
#[derive(Debug, Deserialize, Clone)]
pub struct Config {
    #[serde(default)]
    pub model_list: Vec<ModelEntry>,
    #[serde(default)]
    pub general_settings: GeneralSettings,
    #[serde(default)]
    pub router_settings: RouterSettings,
    #[serde(default)]
    pub server: ServerSettings,
    #[serde(default)]
    pub rate_limit: RateLimitSettings,
}

#[derive(Debug, Deserialize, Clone)]
pub struct ModelEntry {
    pub model_name: String,
    pub litellm_params: ProviderParams,
}

/// Provider params — compatible with litellm's `litellm_params` format.
///
/// The `model` field follows litellm's `provider/model-id` convention:
///   - `openai/gpt-4` → OpenAI provider, model `gpt-4`
///   - `anthropic/claude-sonnet-4-20250514` → Anthropic
///   - `azure/my-deployment` → Azure OpenAI
///   - `gemini/gemini-2.0-flash` → Google Gemini
///   - `bedrock/anthropic.claude-3-sonnet` → AWS Bedrock
///   - `gpt-4` → auto-detected as OpenAI
///   - `claude-3-opus` → auto-detected as Anthropic
#[derive(Debug, Deserialize, Clone)]
pub struct ProviderParams {
    /// Model string in litellm format: `[provider/]model-id`.
    pub model: String,
    /// API key (supports ${ENV_VAR} and os.environ/VAR syntax).
    pub api_key: Option<String>,
    /// API base URL override.
    pub api_base: Option<String>,
    /// Azure OpenAI API version.
    pub api_version: Option<String>,
    /// AWS region for Bedrock.
    pub aws_region_name: Option<String>,
    /// AWS access key ID.
    pub aws_access_key_id: Option<String>,
    /// AWS secret access key.
    pub aws_secret_access_key: Option<String>,
    /// RPM limit for this deployment.
    pub rpm: Option<u64>,
    /// TPM limit for this deployment.
    pub tpm: Option<u64>,
    /// Request timeout in seconds.
    #[serde(default = "default_timeout")]
    pub timeout: u64,
    /// Custom headers to send with every request.
    #[serde(default)]
    pub headers: HashMap<String, String>,
    /// Temperature override.
    pub temperature: Option<f64>,
    /// Max tokens override.
    pub max_tokens: Option<u32>,
}

impl ProviderParams {
    /// Parse the `model` field to determine provider type and actual model ID.
    ///
    /// Returns `(provider_type, actual_model_id)`.
    pub fn resolve_provider_and_model(&self) -> (String, String) {
        if let Some((provider, model)) = self.model.split_once('/') {
            // Explicit prefix: `openai/gpt-4`, `anthropic/claude-3`, etc.
            (provider.to_string(), model.to_string())
        } else {
            // No prefix — auto-detect from model name.
            let provider = auto_detect_provider(&self.model);
            (provider, self.model.clone())
        }
    }
}

fn default_timeout() -> u64 {
    120
}

/// Auto-detect provider from model name when no explicit prefix is given.
fn auto_detect_provider(model: &str) -> String {
    let lower = model.to_lowercase();

    // OpenAI models
    if lower.starts_with("gpt-")
        || lower.starts_with("o1-")
        || lower.starts_with("o3-")
        || lower.starts_with("o4-")
        || lower.starts_with("text-")
        || lower.starts_with("dall-e-")
        || lower.starts_with("chatgpt-")
        || lower.starts_with("ft:gpt-")
    {
        return "openai".to_string();
    }

    // Anthropic models
    if lower.starts_with("claude-") {
        return "anthropic".to_string();
    }

    // Google models
    if lower.starts_with("gemini-") || lower.starts_with("gemma-") {
        return "gemini".to_string();
    }

    // Bedrock models (common prefixes)
    if lower.starts_with("anthropic.")
        || lower.starts_with("amazon.")
        || lower.starts_with("meta.")
        || lower.starts_with("cohere.")
        || lower.starts_with("ai21.")
        || lower.starts_with("mistral.")
    {
        return "bedrock".to_string();
    }

    // Azure — typically uses explicit prefix, but if someone names their deployment
    // differently, they should use `azure/deployment-name`.
    // Default fallback to openai.
    tracing::warn!(
        "Could not auto-detect provider for model '{}', defaulting to openai",
        model
    );
    "openai".to_string()
}

#[derive(Debug, Deserialize, Clone)]
pub struct GeneralSettings {
    /// Master key for admin access.
    pub master_key: Option<String>,
    /// PostgreSQL database URL (compatible with litellm schema).
    pub database_url: Option<String>,
}

impl Default for GeneralSettings {
    fn default() -> Self {
        Self {
            master_key: None,
            database_url: None,
        }
    }
}

#[derive(Debug, Deserialize, Clone, Default)]
pub struct RouterSettings {
    /// Routing strategy: round_robin, least_latency, least_busy, fallback.
    #[serde(default = "default_routing_strategy")]
    pub routing_strategy: String,
}

fn default_routing_strategy() -> String {
    "round_robin".to_string()
}

#[derive(Debug, Deserialize, Clone)]
pub struct ServerSettings {
    #[serde(default = "default_host")]
    pub host: String,
    #[serde(default = "default_port")]
    pub port: u16,
    #[serde(default = "default_workers")]
    pub workers: usize,
}

impl Default for ServerSettings {
    fn default() -> Self {
        Self {
            host: default_host(),
            port: default_port(),
            workers: default_workers(),
        }
    }
}

fn default_host() -> String {
    "0.0.0.0".to_string()
}
fn default_port() -> u16 {
    4000
}
fn default_workers() -> usize {
    4
}

#[derive(Debug, Deserialize, Clone)]
pub struct RateLimitSettings {
    #[serde(default = "default_true")]
    pub enabled: bool,
    /// Default RPM per key if not set in the database.
    #[serde(default = "default_rpm")]
    pub default_rpm: u64,
    /// Custom window limits: [[count, window_seconds], ...].
    /// Example: [[100, 18000]] = 100 requests per 5 hours.
    #[serde(default)]
    pub window_limits: Vec<Vec<u64>>,
}

impl Default for RateLimitSettings {
    fn default() -> Self {
        Self {
            enabled: true,
            default_rpm: default_rpm(),
            window_limits: vec![],
        }
    }
}

fn default_true() -> bool {
    true
}
fn default_rpm() -> u64 {
    60
}

/// Resolve environment variable references in a string.
/// Supports both `${VAR_NAME}` and `os.environ/VAR_NAME` syntax.
pub fn resolve_env_value(value: &str) -> String {
    let trimmed = value.trim();
    if let Some(var_name) = trimmed.strip_prefix("${").and_then(|s| s.strip_suffix('}')) {
        env::var(var_name).unwrap_or_else(|_| value.to_string())
    } else if let Some(var_name) = trimmed.strip_prefix("os.environ/") {
        env::var(var_name).unwrap_or_else(|_| value.to_string())
    } else {
        value.to_string()
    }
}

/// Load config from a YAML file with env var resolution.
/// Compatible with litellm's `proxy_server_config.yaml`.
pub fn load_config(path: &str) -> Result<Config, GatewayError> {
    let content = std::fs::read_to_string(path)
        .map_err(|e| GatewayError::ConfigError(format!("Failed to read config {}: {}", path, e)))?;

    // Resolve env vars in raw YAML before parsing.
    let resolved = resolve_env_vars_in_text(&content);

    let mut config: Config = serde_yaml::from_str(&resolved)
        .map_err(|e| GatewayError::ConfigError(format!("Failed to parse config: {}", e)))?;

    // Resolve env vars in string fields after parsing.
    config.general_settings.master_key = config
        .general_settings
        .master_key
        .take()
        .map(|v| resolve_env_value(&v));
    config.general_settings.database_url = config
        .general_settings
        .database_url
        .take()
        .map(|v| resolve_env_value(&v));

    for entry in &mut config.model_list {
        let p = &mut entry.litellm_params;
        p.api_key = p.api_key.take().map(|v| resolve_env_value(&v));
        p.api_base = p.api_base.take().map(|v| resolve_env_value(&v));
        p.aws_access_key_id = p.aws_access_key_id.take().map(|v| resolve_env_value(&v));
        p.aws_secret_access_key = p.aws_secret_access_key.take().map(|v| resolve_env_value(&v));
        for v in p.headers.values_mut() {
            *v = resolve_env_value(v);
        }
    }

    tracing::info!(
        "Config loaded: {} model(s), strategy={}",
        config.model_list.len(),
        config.router_settings.routing_strategy,
    );

    Ok(config)
}

/// Replace ${VAR} and os.environ/VAR patterns in raw text.
fn resolve_env_vars_in_text(text: &str) -> String {
    let mut result = String::with_capacity(text.len());
    let mut chars = text.char_indices().peekable();
    let text_bytes = text.as_bytes();

    while let Some((i, ch)) = chars.next() {
        if ch == '$' && i + 1 < text.len() && text_bytes[i + 1] == b'{' {
            if let Some(end) = find_closing_brace(text, i + 2) {
                let var_name = &text[i + 2..end];
                if is_valid_env_var(var_name) {
                    result.push_str(
                        &env::var(var_name).unwrap_or_else(|_| format!("${{{}}}", var_name)),
                    );
                    while let Some((j, _)) = chars.next() {
                        if j >= end {
                            break;
                        }
                    }
                    continue;
                }
            }
        }
        result.push(ch);
    }

    // Handle os.environ/VAR patterns
    result = result
        .split("os.environ/")
        .enumerate()
        .map(|(i, part)| {
            if i == 0 {
                part.to_string()
            } else {
                let end = part
                    .find(|c: char| !c.is_alphanumeric() && c != '_')
                    .unwrap_or(part.len());
                let var_name = &part[..end];
                let rest = &part[end..];
                let resolved =
                    env::var(var_name).unwrap_or_else(|_| format!("os.environ/{}", var_name));
                format!("{}{}", resolved, rest)
            }
        })
        .collect();

    result
}

fn find_closing_brace(text: &str, start: usize) -> Option<usize> {
    for (i, ch) in text[start..].char_indices() {
        if ch == '}' {
            return Some(start + i);
        }
        if !ch.is_alphanumeric() && ch != '_' {
            return None;
        }
    }
    None
}

fn is_valid_env_var(s: &str) -> bool {
    !s.is_empty() && s.chars().all(|c| c.is_alphanumeric() || c == '_')
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_explicit_provider_prefix() {
        let params = ProviderParams {
            model: "openai/gpt-4-turbo".to_string(),
            api_key: None,
            api_base: None,
            api_version: None,
            aws_region_name: None,
            aws_access_key_id: None,
            aws_secret_access_key: None,
            rpm: None,
            tpm: None,
            timeout: 120,
            headers: HashMap::new(),
            temperature: None,
            max_tokens: None,
        };
        let (provider, model) = params.resolve_provider_and_model();
        assert_eq!(provider, "openai");
        assert_eq!(model, "gpt-4-turbo");
    }

    #[test]
    fn test_auto_detect_openai() {
        let params = ProviderParams {
            model: "gpt-4o".to_string(),
            api_key: None,
            api_base: None,
            api_version: None,
            aws_region_name: None,
            aws_access_key_id: None,
            aws_secret_access_key: None,
            rpm: None,
            tpm: None,
            timeout: 120,
            headers: HashMap::new(),
            temperature: None,
            max_tokens: None,
        };
        let (provider, model) = params.resolve_provider_and_model();
        assert_eq!(provider, "openai");
        assert_eq!(model, "gpt-4o");
    }

    #[test]
    fn test_auto_detect_anthropic() {
        assert_eq!(auto_detect_provider("claude-3-opus"), "anthropic");
    }

    #[test]
    fn test_auto_detect_gemini() {
        assert_eq!(auto_detect_provider("gemini-2.0-flash"), "gemini");
    }
}

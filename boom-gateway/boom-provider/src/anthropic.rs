use crate::{generate_response_id, now_timestamp};
use boom_core::provider::Provider;
use boom_core::types::*;
use boom_core::GatewayError;
use async_trait::async_trait;
use futures::stream::StreamExt;
use reqwest::Client;
use tokio_stream::wrappers::ReceiverStream;

/// Anthropic provider.
///
/// Anthropic's Messages API differs from OpenAI:
/// - System prompt is a top-level field, not a message.
/// - Content uses content blocks (array of {type: "text", text: "..."}).
/// - Streaming uses different SSE event types.
pub struct AnthropicProvider {
    client: Client,
    api_key: Option<String>,
    base_url: String,
    model: String,
}

impl AnthropicProvider {
    pub fn new(
        client: Client,
        api_key: Option<String>,
        api_base: Option<String>,
        model: &str,
    ) -> Self {
        Self {
            client,
            api_key,
            base_url: api_base
                .unwrap_or_else(|| "https://api.anthropic.com/v1".to_string()),
            model: model.to_string(),
        }
    }

    /// Convert our ChatCompletionRequest to Anthropic's format.
    fn to_anthropic_request(&self, req: &ChatCompletionRequest) -> serde_json::Value {
        let mut system_prompt = String::new();
        let mut messages = Vec::new();

        for msg in &req.messages {
            match msg.role {
                MessageRole::System => {
                    // Anthropic puts system as a top-level field.
                    system_prompt = match &msg.content {
                        MessageContent::Text(t) => t.clone(),
                        MessageContent::Parts(parts) => parts
                            .iter()
                            .filter_map(|p| match p {
                                ContentPart::Text { text } => Some(text.as_str()),
                                _ => None,
                            })
                            .collect::<Vec<_>>()
                            .join("\n"),
                        MessageContent::Null => String::new(),
                    };
                }
                _ => {
                    let content = match &msg.content {
                        MessageContent::Text(t) => {
                            serde_json::json!([{"type": "text", "text": t}])
                        }
                        MessageContent::Parts(parts) => {
                            serde_json::json!(parts)
                        }
                        MessageContent::Null => serde_json::json!([]),
                    };
                    messages.push(serde_json::json!({
                        "role": match msg.role {
                            MessageRole::User => "user",
                            MessageRole::Assistant => "assistant",
                            _ => "user",
                        },
                        "content": content,
                    }));
                }
            }
        }

        let mut body = serde_json::json!({
            "model": self.model,
            "messages": messages,
            "max_tokens": req.max_completion_tokens.or(req.max_tokens).unwrap_or(4096),
        });

        if !system_prompt.is_empty() {
            body["system"] = serde_json::json!(system_prompt);
        }
        if let Some(temp) = req.temperature {
            body["temperature"] = serde_json::json!(temp);
        }
        if let Some(top_p) = req.top_p {
            body["top_p"] = serde_json::json!(top_p);
        }
        if let Some(n) = req.n {
            body["n"] = serde_json::json!(n);
        }

        body
    }

    /// Convert Anthropic's response to OpenAI format.
    fn from_anthropic_response(
        &self,
        resp: serde_json::Value,
        requested_model: &str,
    ) -> ChatCompletionResponse {
        let content_blocks = resp
            .get("content")
            .and_then(|c| c.as_array())
            .map(|blocks| {
                blocks
                    .iter()
                    .filter_map(|b| b.get("text").and_then(|t| t.as_str()).map(String::from))
                    .collect::<Vec<_>>()
                    .join("")
            })
            .unwrap_or_default();

        let stop_reason = resp
            .get("stop_reason")
            .and_then(|s| s.as_str())
            .map(String::from);

        let usage = resp.get("usage");
        let input_tokens = usage
            .and_then(|u| u.get("input_tokens"))
            .and_then(|t| t.as_u64())
            .unwrap_or(0) as u32;
        let output_tokens = usage
            .and_then(|u| u.get("output_tokens"))
            .and_then(|t| t.as_u64())
            .unwrap_or(0) as u32;

        ChatCompletionResponse {
            id: resp
                .get("id")
                .and_then(|i| i.as_str())
                .map(String::from)
                .unwrap_or_else(generate_response_id),
            object: "chat.completion".to_string(),
            created: now_timestamp(),
            model: requested_model.to_string(),
            choices: vec![Choice {
                index: 0,
                message: Message {
                    role: MessageRole::Assistant,
                    content: MessageContent::Text(content_blocks),
                    name: None,
                    tool_calls: None,
                    tool_call_id: None,
                },
                finish_reason: stop_reason.or_else(|| Some("stop".to_string())),
                logprobs: None,
            }],
            usage: Usage {
                prompt_tokens: input_tokens,
                completion_tokens: output_tokens,
                total_tokens: input_tokens + output_tokens,
            },
            system_fingerprint: None,
        }
    }
}

#[async_trait]
impl Provider for AnthropicProvider {
    async fn chat(&self, req: ChatCompletionRequest) -> Result<ChatCompletionResponse, GatewayError> {
        let requested_model = req.model.clone();
        let body = self.to_anthropic_request(&req);
        let url = format!("{}/messages", self.base_url.trim_end_matches('/'));

        let mut builder = self.client.post(&url);
        if let Some(ref key) = self.api_key {
            builder = builder.header("x-api-key", key);
        }
        builder = builder
            .header("anthropic-version", "2023-06-01")
            .header("content-type", "application/json");

        let resp = builder
            .json(&body)
            .send()
            .await
            .map_err(|e| GatewayError::ProviderError(format!("Anthropic request failed: {}", e)))?;

        let status = resp.status();
        if !status.is_success() {
            let error_body = resp.text().await.unwrap_or_default();
            return Err(GatewayError::UpstreamError {
                status: status.as_u16(),
                message: error_body,
            });
        }

        let body: serde_json::Value = resp
            .json()
            .await
            .map_err(|e| GatewayError::ProviderError(format!("Failed to parse Anthropic response: {}", e)))?;

        Ok(self.from_anthropic_response(body, &requested_model))
    }

    async fn chat_stream(&self, req: ChatCompletionRequest) -> Result<ChatStream, GatewayError> {
        let requested_model = req.model.clone();
        let mut body = self.to_anthropic_request(&req);
        body["stream"] = serde_json::json!(true);

        let url = format!("{}/messages", self.base_url.trim_end_matches('/'));

        let mut builder = self.client.post(&url);
        if let Some(ref key) = self.api_key {
            builder = builder.header("x-api-key", key);
        }
        builder = builder
            .header("anthropic-version", "2023-06-01")
            .header("content-type", "application/json");

        let resp = builder
            .json(&body)
            .send()
            .await
            .map_err(|e| GatewayError::ProviderError(format!("Anthropic stream request failed: {}", e)))?;

        let status = resp.status();
        if !status.is_success() {
            let error_body = resp.text().await.unwrap_or_default();
            return Err(GatewayError::UpstreamError {
                status: status.as_u16(),
                message: error_body,
            });
        }

        let (tx, rx) = tokio::sync::mpsc::channel(32);
        let model = requested_model;

        tokio::spawn(async move {
            let mut stream = resp.bytes_stream();
            let mut buffer = String::new();
            let response_id = generate_response_id();
            let created = now_timestamp();

            while let Some(chunk_result) = stream.next().await {
                match chunk_result {
                    Ok(bytes) => {
                        buffer.push_str(&String::from_utf8_lossy(&bytes));

                        while let Some(pos) = buffer.find("\n\n") {
                            let event_text = buffer[..pos].to_string();
                            buffer = buffer[pos + 2..].to_string();

                            let mut event_type = String::new();
                            let mut data_str = String::new();

                            for line in event_text.lines() {
                                if let Some(val) = line.strip_prefix("event: ") {
                                    event_type = val.trim().to_string();
                                } else if let Some(val) = line.strip_prefix("data: ") {
                                    data_str = val.trim().to_string();
                                }
                            }

                            if data_str.is_empty() {
                                continue;
                            }

                            match event_type.as_str() {
                                "content_block_delta" => {
                                    if let Ok(data) =
                                        serde_json::from_str::<serde_json::Value>(&data_str)
                                    {
                                        let text = data
                                            .get("delta")
                                            .and_then(|d| d.get("text"))
                                            .and_then(|t| t.as_str())
                                            .unwrap_or("");

                                        let chunk = ChatStreamChunk {
                                            id: response_id.clone(),
                                            object: "chat.completion.chunk".to_string(),
                                            created,
                                            model: model.clone(),
                                            choices: vec![StreamChoice {
                                                index: 0,
                                                delta: StreamDelta {
                                                    role: None,
                                                    content: Some(text.to_string()),
                                                    tool_calls: None,
                                                },
                                                finish_reason: None,
                                            }],
                                            usage: None,
                                        };
                                        if tx.send(Ok(Some(chunk))).await.is_err() {
                                            return;
                                        }
                                    }
                                }
                                "message_stop" => {
                                    let _ = tx.send(Ok(None)).await;
                                    return;
                                }
                                "message_start" | "message_delta" | "content_block_start"
                                | "content_block_stop" | "ping" => {
                                    // Forward relevant events, skip others.
                                }
                                _ => {}
                            }
                        }
                    }
                    Err(e) => {
                        let _ = tx
                            .send(Err(GatewayError::ProviderError(format!(
                                "Stream error: {}",
                                e
                            ))))
                            .await;
                        return;
                    }
                }
            }
        });

        let stream = ReceiverStream::new(rx).filter_map(|result| async move {
            match result {
                Ok(Some(chunk)) => Some(Ok(chunk)),
                Ok(None) => None,
                Err(e) => Some(Err(e)),
            }
        });

        Ok(Box::pin(stream))
    }

    fn name(&self) -> &str {
        "anthropic"
    }

    fn models(&self) -> &[String] {
        std::slice::from_ref(&self.model)
    }
}

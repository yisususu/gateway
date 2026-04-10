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
    deployment_id: Option<String>,
}

impl AnthropicProvider {
    pub fn new(
        client: Client,
        api_key: Option<String>,
        api_base: Option<String>,
        model: &str,
        deployment_id: Option<String>,
    ) -> Self {
        Self {
            client,
            api_key,
            base_url: api_base
                .unwrap_or_else(|| "https://api.anthropic.com/v1".to_string()),
            model: model.to_string(),
            deployment_id,
        }
    }

    /// Convert our ChatCompletionRequest to Anthropic's format.
    fn to_anthropic_request(&self, req: &ChatCompletionRequest) -> serde_json::Value {
        let mut system_prompt = String::new();
        let system_blocks: Vec<serde_json::Value> = Vec::new();
        let mut messages = Vec::new();

        for msg in &req.messages {
            match msg.role {
                MessageRole::System => {
                    // Anthropic puts system as a top-level field.
                    match &msg.content {
                        MessageContent::Text(t) => system_prompt = t.clone(),
                        MessageContent::Parts(parts) => {
                            system_prompt = parts
                                .iter()
                                .filter_map(|p| match p {
                                    ContentPart::Text { text } => Some(text.as_str()),
                                    _ => None,
                                })
                                .collect::<Vec<_>>()
                                .join("\n");
                        }
                        MessageContent::Null => {}
                    }
                }
                MessageRole::Assistant => {
                    let mut content_blocks: Vec<serde_json::Value> = Vec::new();

                    // Text content.
                    match &msg.content {
                        MessageContent::Text(t) if !t.is_empty() => {
                            content_blocks
                                .push(serde_json::json!({"type": "text", "text": t}));
                        }
                        MessageContent::Parts(parts) => {
                            for p in parts {
                                if let ContentPart::Text { text } = p {
                                    content_blocks.push(
                                        serde_json::json!({"type": "text", "text": text}),
                                    );
                                }
                            }
                        }
                        _ => {}
                    }

                    // Tool calls → tool_use content blocks.
                    if let Some(ref tool_calls) = msg.tool_calls {
                        for tc in tool_calls {
                            let input: serde_json::Value =
                                serde_json::from_str(&tc.function.arguments)
                                    .unwrap_or(serde_json::Value::Object(Default::default()));
                            content_blocks.push(serde_json::json!({
                                "type": "tool_use",
                                "id": tc.id,
                                "name": tc.function.name,
                                "input": input,
                            }));
                        }
                    }

                    // Ensure at least one content block (Anthropic requires non-empty content).
                    if content_blocks.is_empty() {
                        content_blocks.push(serde_json::json!({"type": "text", "text": ""}));
                    }

                    messages.push(serde_json::json!({
                        "role": "assistant",
                        "content": content_blocks,
                    }));
                }
                MessageRole::Tool => {
                    // Tool result → user message with tool_result content block.
                    let text = match &msg.content {
                        MessageContent::Text(t) => t.clone(),
                        MessageContent::Parts(parts) => parts
                            .iter()
                            .filter_map(|p| match p {
                                ContentPart::Text { text } => Some(text.as_str()),
                                _ => None,
                            })
                            .collect::<Vec<_>>()
                            .join(""),
                        MessageContent::Null => String::new(),
                    };
                    let tool_call_id = msg.tool_call_id.clone().unwrap_or_default();
                    messages.push(serde_json::json!({
                        "role": "user",
                        "content": [{
                            "type": "tool_result",
                            "tool_use_id": tool_call_id,
                            "content": text,
                        }],
                    }));
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
                        "role": "user",
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

        // System prompt.
        if !system_prompt.is_empty() {
            body["system"] = serde_json::json!(system_prompt);
        } else if !system_blocks.is_empty() {
            body["system"] = serde_json::json!(system_blocks);
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

        // Stop sequences.
        if let Some(ref stop) = req.stop {
            let seqs: Vec<String> = match stop {
                StopSequence::Single(s) => vec![s.clone()],
                StopSequence::Multiple(v) => v.clone(),
            };
            body["stop_sequences"] = serde_json::json!(seqs);
        }

        // Tools: OpenAI format → Anthropic format.
        if let Some(ref tools) = req.tools {
            let anthropic_tools: Vec<serde_json::Value> = tools
                .iter()
                .map(|t| {
                    let mut tool = serde_json::json!({
                        "name": t.function.name,
                        "input_schema": t.function.parameters,
                    });
                    if let Some(ref desc) = t.function.description {
                        tool["description"] = serde_json::json!(desc);
                    }
                    tool
                })
                .collect();
            body["tools"] = serde_json::json!(anthropic_tools);
        }

        // Tool choice.
        if let Some(ref tc) = req.tool_choice {
            body["tool_choice"] = serde_json::json!(tc);
        }

        // Forward extra fields: thinking, metadata, etc.
        if let Some(thinking) = req.extra.get("thinking") {
            body["thinking"] = thinking.clone();
        }
        if let Some(metadata) = req.extra.get("metadata") {
            body["metadata"] = metadata.clone();
        }

        body
    }

    /// Convert Anthropic's response to OpenAI format.
    fn from_anthropic_response(
        &self,
        resp: serde_json::Value,
        requested_model: &str,
    ) -> ChatCompletionResponse {
        let mut text_parts = Vec::new();
        let mut tool_calls = Vec::new();

        if let Some(blocks) = resp.get("content").and_then(|c| c.as_array()) {
            for block in blocks {
                let block_type = block.get("type").and_then(|t| t.as_str()).unwrap_or("");
                match block_type {
                    "text" => {
                        if let Some(text) = block.get("text").and_then(|t| t.as_str()) {
                            text_parts.push(text.to_string());
                        }
                    }
                    "tool_use" => {
                        let id = block
                            .get("id")
                            .and_then(|i| i.as_str())
                            .unwrap_or("")
                            .to_string();
                        let name = block
                            .get("name")
                            .and_then(|n| n.as_str())
                            .unwrap_or("")
                            .to_string();
                        let input = block.get("input").cloned().unwrap_or(serde_json::json!({}));
                        let arguments = serde_json::to_string(&input).unwrap_or_default();
                        tool_calls.push(ToolCall {
                            id,
                            call_type: "function".to_string(),
                            function: FunctionCall { name, arguments },
                        });
                    }
                    _ => {}
                }
            }
        }

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

        let finish_reason = stop_reason
            .map(|r| match r.as_str() {
                "tool_use" => "tool_calls".to_string(),
                "end_turn" | "stop" => "stop".to_string(),
                "max_tokens" => "length".to_string(),
                other => other.to_string(),
            })
            .or_else(|| Some("stop".to_string()));

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
                    content: MessageContent::Text(text_parts.join("")),
                    name: None,
                    tool_calls: if tool_calls.is_empty() {
                        None
                    } else {
                        Some(tool_calls)
                    },
                    tool_call_id: None,
                },
                finish_reason,
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

        // Non-streaming: upstream sends no data until the entire response is ready.
        let resp = builder
            .timeout(std::time::Duration::from_secs(600))
            .json(&body)
            .send()
            .await
            .map_err(|e| {
                tracing::error!("Anthropic request failed: {}", e);
                GatewayError::ProviderError("Upstream provider unavailable".to_string())
            })?;

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
            .map_err(|e| {
                tracing::error!("Failed to parse Anthropic response: {}", e);
                GatewayError::ProviderError("Failed to process upstream response".to_string())
            })?;

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
            .map_err(|e| {
                tracing::error!("Anthropic stream request failed: {}", e);
                GatewayError::ProviderError("Upstream provider unavailable".to_string())
            })?;

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

            // Track open content blocks by index for proper tool_use handling.
            // Maps Anthropic content_block index → OpenAI tool_call index (0-based).
            let mut tool_index_map: std::collections::HashMap<u32, u32> = std::collections::HashMap::new();
            let mut next_tool_index: u32 = 0;

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

                            let data: serde_json::Value = match serde_json::from_str(&data_str) {
                                Ok(d) => d,
                                Err(_) => continue,
                            };

                            match event_type.as_str() {
                                "content_block_start" => {
                                    let idx = data.get("index").and_then(|i| i.as_u64()).unwrap_or(0) as u32;
                                    let block = data.get("content_block").unwrap_or(&serde_json::Value::Null);
                                    let btype = block.get("type").and_then(|t| t.as_str()).unwrap_or("text").to_string();

                                    // For tool_use blocks, record id and emit a tool_call delta.
                                    if btype == "tool_use" {
                                        let id = block.get("id").and_then(|i| i.as_str()).unwrap_or("").to_string();
                                        let name = block.get("name").and_then(|n| n.as_str()).unwrap_or("").to_string();
                                        let openai_idx = next_tool_index;
                                        tool_index_map.insert(idx, openai_idx);
                                        next_tool_index += 1;

                                        let chunk = ChatStreamChunk {
                                            id: response_id.clone(),
                                            object: "chat.completion.chunk".to_string(),
                                            created,
                                            model: model.clone(),
                                            choices: vec![StreamChoice {
                                                index: 0,
                                                delta: StreamDelta {
                                                    role: None,
                                                    content: None,
                                                    tool_calls: Some(vec![ToolCallDelta {
                                                        index: openai_idx,
                                                        id: Some(id),
                                                        call_type: Some("function".to_string()),
                                                        function: Some(FunctionCallDelta {
                                                            name: Some(name),
                                                            arguments: Some(String::new()),
                                                        }),
                                                    }]),
                                                },
                                                finish_reason: None,
                                            }],
                                            usage: None,
                                        };
                                        if tx.send(Ok(Some(chunk))).await.is_err() {
                                            return;
                                        }
                                    }
                                    // Text blocks: nothing to emit on start.
                                }
                                "content_block_delta" => {
                                    let idx = data.get("index").and_then(|i| i.as_u64()).unwrap_or(0) as u32;
                                    let delta = data.get("delta").unwrap_or(&serde_json::Value::Null);
                                    let delta_type = delta.get("type").and_then(|t| t.as_str()).unwrap_or("");

                                    match delta_type {
                                        "text_delta" => {
                                            let text = delta.get("text").and_then(|t| t.as_str()).unwrap_or("");
                                            if !text.is_empty() {
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
                                        "input_json_delta" => {
                                            // Tool argument fragment.
                                            let partial = delta.get("partial_json").and_then(|p| p.as_str()).unwrap_or("");
                                            let openai_idx = tool_index_map.get(&idx).copied().unwrap_or(idx);
                                            let chunk = ChatStreamChunk {
                                                id: response_id.clone(),
                                                object: "chat.completion.chunk".to_string(),
                                                created,
                                                model: model.clone(),
                                                choices: vec![StreamChoice {
                                                    index: 0,
                                                    delta: StreamDelta {
                                                        role: None,
                                                        content: None,
                                                        tool_calls: Some(vec![ToolCallDelta {
                                                            index: openai_idx,
                                                            id: None,
                                                            call_type: None,
                                                            function: Some(FunctionCallDelta {
                                                                name: None,
                                                                arguments: Some(partial.to_string()),
                                                            }),
                                                        }]),
                                                    },
                                                    finish_reason: None,
                                                }],
                                                usage: None,
                                            };
                                            if tx.send(Ok(Some(chunk))).await.is_err() {
                                                return;
                                            }
                                        }
                                        _ => {}
                                    }
                                }
                                "message_delta" => {
                                    // Final event: stop_reason + usage.
                                    let stop_reason = data
                                        .get("delta")
                                        .and_then(|d| d.get("stop_reason"))
                                        .and_then(|s| s.as_str());
                                    let finish_reason = stop_reason.map(|r| match r {
                                        "tool_use" => "tool_calls".to_string(),
                                        "end_turn" | "stop" => "stop".to_string(),
                                        "max_tokens" => "length".to_string(),
                                        other => other.to_string(),
                                    });

                                    let usage_data = data.get("usage");
                                    let output_tokens = usage_data
                                        .and_then(|u| u.get("output_tokens"))
                                        .and_then(|t| t.as_u64())
                                        .unwrap_or(0) as i32;

                                    let chunk = ChatStreamChunk {
                                        id: response_id.clone(),
                                        object: "chat.completion.chunk".to_string(),
                                        created,
                                        model: model.clone(),
                                        choices: vec![StreamChoice {
                                            index: 0,
                                            delta: StreamDelta {
                                                role: None,
                                                content: None,
                                                tool_calls: None,
                                            },
                                            finish_reason,
                                        }],
                                        usage: Some(StreamUsage {
                                            prompt_tokens: None,
                                            completion_tokens: Some(output_tokens),
                                            total_tokens: None,
                                        }),
                                    };
                                    if tx.send(Ok(Some(chunk))).await.is_err() {
                                        return;
                                    }
                                }
                                "message_start" => {
                                    // Extract input_tokens from message_start for usage tracking.
                                    // No chunk emitted — role is set on first content.
                                }
                                "message_stop" => {
                                    let _ = tx.send(Ok(None)).await;
                                    return;
                                }
                                "content_block_stop" | "ping" => {
                                    // No action needed.
                                }
                                _ => {}
                            }
                        }
                    }
                    Err(e) => {
                        tracing::error!("Anthropic stream read error: {}", e);
                        let _ = tx
                            .send(Err(GatewayError::ProviderError(
                                "Upstream stream error".to_string(),
                            )))
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

    fn deployment_id(&self) -> Option<&str> {
        self.deployment_id.as_deref()
    }
}

use crate::{generate_response_id, now_timestamp};
use boom_core::provider::Provider;
use boom_core::types::*;
use boom_core::GatewayError;
use async_trait::async_trait;
use futures::stream::StreamExt;
use reqwest::Client;
use tokio_stream::wrappers::ReceiverStream;

/// Google Gemini provider.
///
/// Gemini uses a very different API format from OpenAI.
/// The key is passed as a query parameter, not a header.
pub struct GeminiProvider {
    client: Client,
    api_key: Option<String>,
    model: String,
}

impl GeminiProvider {
    pub fn new(client: Client, api_key: Option<String>, model: &str) -> Self {
        Self {
            client,
            api_key,
            model: model.to_string(),
        }
    }

    /// Convert to Gemini's generateContent format.
    fn to_gemini_request(&self, req: &ChatCompletionRequest) -> serde_json::Value {
        let mut system_instruction = None;
        let mut contents = Vec::new();

        for msg in &req.messages {
            let text = match &msg.content {
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

            match msg.role {
                MessageRole::System => {
                    system_instruction = Some(serde_json::json!({
                        "parts": [{"text": text}]
                    }));
                }
                MessageRole::User => {
                    contents.push(serde_json::json!({
                        "role": "user",
                        "parts": [{"text": text}]
                    }));
                }
                MessageRole::Assistant => {
                    contents.push(serde_json::json!({
                        "role": "model",
                        "parts": [{"text": text}]
                    }));
                }
                _ => {}
            }
        }

        let mut body = serde_json::json!({
            "contents": contents,
        });

        if let Some(si) = system_instruction {
            body["systemInstruction"] = si;
        }

        // Build generationConfig.
        let mut config = serde_json::Map::new();
        if let Some(temp) = req.temperature {
            config.insert("temperature".to_string(), serde_json::json!(temp));
        }
        if let Some(top_p) = req.top_p {
            config.insert("topP".to_string(), serde_json::json!(top_p));
        }
        if let Some(max_tokens) = req.max_completion_tokens.or(req.max_tokens) {
            config.insert("maxOutputTokens".to_string(), serde_json::json!(max_tokens));
        }
        if let Some(n) = req.n {
            config.insert("candidateCount".to_string(), serde_json::json!(n));
        }
        if let Some(stop) = &req.stop {
            let stops: Vec<String> = match stop {
                StopSequence::Single(s) => vec![s.clone()],
                StopSequence::Multiple(v) => v.clone(),
            };
            config.insert("stopSequences".to_string(), serde_json::json!(stops));
        }

        if !config.is_empty() {
            body["generationConfig"] = serde_json::Value::Object(config);
        }

        body
    }

    fn from_gemini_response(
        &self,
        resp: serde_json::Value,
        requested_model: &str,
    ) -> ChatCompletionResponse {
        let text = resp
            .get("candidates")
            .and_then(|c| c.get(0))
            .and_then(|c| c.get("content"))
            .and_then(|c| c.get("parts"))
            .and_then(|p| p.get(0))
            .and_then(|p| p.get("text"))
            .and_then(|t| t.as_str())
            .unwrap_or("")
            .to_string();

        let finish_reason = resp
            .get("candidates")
            .and_then(|c| c.get(0))
            .and_then(|c| c.get("finishReason"))
            .and_then(|r| r.as_str())
            .map(|r| match r {
                "STOP" => "stop",
                "MAX_TOKENS" => "length",
                other => other,
            })
            .map(String::from);

        let usage_meta = resp.get("usageMetadata");
        let prompt_tokens = usage_meta
            .and_then(|u| u.get("promptTokenCount"))
            .and_then(|t| t.as_u64())
            .unwrap_or(0) as u32;
        let completion_tokens = usage_meta
            .and_then(|u| u.get("candidatesTokenCount"))
            .and_then(|t| t.as_u64())
            .unwrap_or(0) as u32;

        ChatCompletionResponse {
            id: generate_response_id(),
            object: "chat.completion".to_string(),
            created: now_timestamp(),
            model: requested_model.to_string(),
            choices: vec![Choice {
                index: 0,
                message: Message {
                    role: MessageRole::Assistant,
                    content: MessageContent::Text(text),
                    name: None,
                    tool_calls: None,
                    tool_call_id: None,
                },
                finish_reason,
                logprobs: None,
            }],
            usage: Usage {
                prompt_tokens,
                completion_tokens,
                total_tokens: prompt_tokens + completion_tokens,
            },
            system_fingerprint: None,
        }
    }
}

#[async_trait]
impl Provider for GeminiProvider {
    async fn chat(&self, req: ChatCompletionRequest) -> Result<ChatCompletionResponse, GatewayError> {
        let requested_model = req.model.clone();
        let body = self.to_gemini_request(&req);
        let url = format!(
            "https://generativelanguage.googleapis.com/v1beta/models/{}:generateContent",
            self.model
        );

        let mut builder = self.client.post(&url);
        if let Some(ref key) = self.api_key {
            builder = builder.query(&[("key", key)]);
        }

        let resp = builder
            .json(&body)
            .send()
            .await
            .map_err(|e| GatewayError::ProviderError(format!("Gemini request failed: {}", e)))?;

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
            .map_err(|e| GatewayError::ProviderError(format!("Failed to parse Gemini response: {}", e)))?;

        Ok(self.from_gemini_response(body, &requested_model))
    }

    async fn chat_stream(&self, req: ChatCompletionRequest) -> Result<ChatStream, GatewayError> {
        let requested_model = req.model.clone();
        let body = self.to_gemini_request(&req);
        let url = format!(
            "https://generativelanguage.googleapis.com/v1beta/models/{}:streamGenerateContent?alt=sse",
            self.model
        );

        let mut builder = self.client.post(&url);
        if let Some(ref key) = self.api_key {
            builder = builder.query(&[("key", key)]);
        }

        let resp = builder
            .json(&body)
            .send()
            .await
            .map_err(|e| GatewayError::ProviderError(format!("Gemini stream request failed: {}", e)))?;

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

                            for line in event_text.lines() {
                                if let Some(data) = line.strip_prefix("data: ") {
                                    let data = data.trim();
                                    if let Ok(gemini_resp) =
                                        serde_json::from_str::<serde_json::Value>(data)
                                    {
                                        let text = gemini_resp
                                            .get("candidates")
                                            .and_then(|c| c.get(0))
                                            .and_then(|c| c.get("content"))
                                            .and_then(|c| c.get("parts"))
                                            .and_then(|p| p.get(0))
                                            .and_then(|p| p.get("text"))
                                            .and_then(|t| t.as_str())
                                            .unwrap_or("");

                                        if text.is_empty() {
                                            continue;
                                        }

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
                                        };
                                        if tx.send(Ok(Some(chunk))).await.is_err() {
                                            return;
                                        }
                                    }
                                }
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
            let _ = tx.send(Ok(None)).await;
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
        "gemini"
    }

    fn models(&self) -> &[String] {
        std::slice::from_ref(&self.model)
    }
}

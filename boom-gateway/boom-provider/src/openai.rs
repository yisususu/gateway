use boom_core::provider::{Provider, RequestContext};
use boom_core::types::*;
use boom_core::GatewayError;
use async_trait::async_trait;
use futures::stream::StreamExt;
use reqwest::Client;
use tokio_stream::wrappers::ReceiverStream;

/// OpenAI provider — also serves as the base for Azure (same API format).
pub struct OpenAIProvider {
    client: Client,
    api_key: Option<String>,
    base_url: String,
    model: String,
    deployment_id: Option<String>,
}

impl OpenAIProvider {
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
                .unwrap_or_else(|| "https://api.openai.com/v1".to_string()),
            model: model.to_string(),
            deployment_id,
        }
    }

    fn build_request(&self, mut req: ChatCompletionRequest) -> serde_json::Value {
        // Replace model name with the actual provider model ID.
        req.model = self.model.clone();
        // Convert internal ContentPart::Reasoning to ContentPart::Text so that
        // upstream OpenAI-compatible APIs don't reject the unknown "reasoning" type.
        for msg in &mut req.messages {
            if let MessageContent::Parts(parts) = &mut msg.content {
                for part in parts.iter_mut() {
                    if let ContentPart::Reasoning { reasoning } = part {
                        *part = ContentPart::Text { text: std::mem::take(reasoning) };
                    }
                }
            }
        }
        // Serialize — skip_serializing on `extra` ensures non-standard fields
        // (service_tier, store, etc.) are NOT forwarded to upstream providers.
        serde_json::to_value(&req).unwrap_or_default()
    }
}

#[async_trait]
impl Provider for OpenAIProvider {
    async fn chat(&self, req: ChatCompletionRequest, ctx: &RequestContext) -> Result<ChatCompletionResponse, GatewayError> {
        let body = self.build_request(req);
        let url = format!("{}/chat/completions", self.base_url.trim_end_matches('/'));

        let mut builder = self.client.post(&url);
        if let Some(ref key) = self.api_key {
            builder = builder.bearer_auth(key);
        }
        builder = builder.header("X-Gateway-Priority", ctx.priority.to_string());

        let resp = builder
            .json(&body)
            .send()
            .await
            .map_err(|e| {
                tracing::error!("OpenAI request failed: {}", e);
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

        resp.json::<ChatCompletionResponse>()
            .await
            .map_err(|e| {
                tracing::error!("Failed to parse OpenAI response: {}", e);
                GatewayError::ProviderError("Failed to process upstream response".to_string())
            })
    }

    async fn chat_stream(&self, req: ChatCompletionRequest, ctx: &RequestContext) -> Result<ChatStream, GatewayError> {
        let mut body = self.build_request(req);
        // Ensure stream is enabled and request usage in the final chunk.
        if let Some(obj) = body.as_object_mut() {
            obj.insert("stream".to_string(), serde_json::Value::Bool(true));
            obj.insert(
                "stream_options".to_string(),
                serde_json::json!({ "include_usage": true }),
            );
        }

        let url = format!("{}/chat/completions", self.base_url.trim_end_matches('/'));

        let mut builder = self.client.post(&url);
        if let Some(ref key) = self.api_key {
            builder = builder.bearer_auth(key);
        }
        builder = builder.header("X-Gateway-Priority", ctx.priority.to_string());

        let resp = builder
            .json(&body)
            .send()
            .await
            .map_err(|e| {
                tracing::error!("OpenAI stream request failed: {}", e);
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

        // Parse SSE byte stream into ChatStreamChunk items.
        let (tx, rx) = tokio::sync::mpsc::channel(32);

        tokio::spawn(async move {
            let mut stream = resp.bytes_stream();
            let mut buffer = String::new();

            while let Some(chunk_result) = stream.next().await {
                match chunk_result {
                    Ok(bytes) => {
                        buffer.push_str(&String::from_utf8_lossy(&bytes));
                        // Process complete SSE lines.
                        while let Some(pos) = buffer.find("\n\n") {
                            let event_text = buffer[..pos].to_string();
                            buffer = buffer[pos + 2..].to_string();

                            for line in event_text.lines() {
                                if let Some(data) = line.strip_prefix("data: ") {
                                    let data = data.trim();
                                    if data == "[DONE]" {
                                        let _ = tx.send(Ok(None)).await;
                                        return;
                                    }
                                    match serde_json::from_str::<ChatStreamChunk>(data) {
                                        Ok(chunk) => {
                                            if tx.send(Ok(Some(chunk))).await.is_err() {
                                                return;
                                            }
                                        }
                                        Err(e) => {
                                            tracing::warn!("Failed to parse SSE chunk: {}", e);
                                        }
                                    }
                                }
                            }
                        }
                    }
                    Err(e) => {
                        tracing::error!("OpenAI stream read error: {}", e);
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
                Ok(None) => None, // [DONE]
                Err(e) => Some(Err(e)),
            }
        });

        Ok(Box::pin(stream))
    }

    fn name(&self) -> &str {
        "openai"
    }

    fn models(&self) -> &[String] {
        std::slice::from_ref(&self.model)
    }

    fn deployment_id(&self) -> Option<&str> {
        self.deployment_id.as_deref()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use wiremock::matchers::{method, path, header};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    fn minimal_chat_request() -> ChatCompletionRequest {
        ChatCompletionRequest {
            model: "test-model".to_string(),
            messages: vec![Message {
                role: MessageRole::User,
                content: MessageContent::Text("hello".to_string()),
                name: None,
                tool_calls: None,
                tool_call_id: None,
                reasoning_content: None,
            }],
            temperature: None,
            top_p: None,
            n: None,
            stream: None,
            stop: None,
            max_tokens: None,
            max_completion_tokens: None,
            presence_penalty: None,
            frequency_penalty: None,
            user: None,
            tools: None,
            tool_choice: None,
            response_format: None,
            seed: None,
            logprobs: None,
            top_logprobs: None,
            logit_bias: None,
            extra: Default::default(),
        }
    }

    fn fake_completion_response() -> serde_json::Value {
        serde_json::json!({
            "id": "chatcmpl-test",
            "object": "chat.completion",
            "created": 1700000000_u64,
            "model": "test-model",
            "choices": [{
                "index": 0,
                "message": {
                    "role": "assistant",
                    "content": "hi"
                },
                "finish_reason": "stop"
            }],
            "usage": {
                "prompt_tokens": 5,
                "completion_tokens": 1,
                "total_tokens": 6
            }
        })
    }

    #[tokio::test]
    async fn chat_sends_priority_header_for_vip() {
        let server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path("/chat/completions"))
            .and(header("X-Gateway-Priority", "100"))
            .respond_with(ResponseTemplate::new(200).set_body_json(fake_completion_response()))
            .expect(1)
            .mount(&server)
            .await;

        let provider = OpenAIProvider::new(
            Client::new(),
            None,
            Some(server.uri()),
            "test-model",
            None,
        );

        let ctx = RequestContext { priority: RequestContext::PRIORITY_VIP };
        let result = provider.chat(minimal_chat_request(), &ctx).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn chat_sends_priority_header_for_normal() {
        let server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path("/chat/completions"))
            .and(header("X-Gateway-Priority", "0"))
            .respond_with(ResponseTemplate::new(200).set_body_json(fake_completion_response()))
            .expect(1)
            .mount(&server)
            .await;

        let provider = OpenAIProvider::new(
            Client::new(),
            None,
            Some(server.uri()),
            "test-model",
            None,
        );

        let ctx = RequestContext::default();
        let result = provider.chat(minimal_chat_request(), &ctx).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn chat_sends_custom_priority_value() {
        let server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path("/chat/completions"))
            .and(header("X-Gateway-Priority", "42"))
            .respond_with(ResponseTemplate::new(200).set_body_json(fake_completion_response()))
            .expect(1)
            .mount(&server)
            .await;

        let provider = OpenAIProvider::new(
            Client::new(),
            None,
            Some(server.uri()),
            "test-model",
            None,
        );

        let ctx = RequestContext { priority: 42 };
        let result = provider.chat(minimal_chat_request(), &ctx).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn chat_stream_sends_priority_header() {
        let sse_body = "data: {\"id\":\"chatcmpl-test\",\"object\":\"chat.completion.chunk\",\"created\":1700000000,\"model\":\"test-model\",\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\",\"content\":\"hi\"},\"finish_reason\":null}]}\n\ndata: [DONE]\n\n";

        let server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path("/chat/completions"))
            .and(header("X-Gateway-Priority", "100"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_string(sse_body)
                    .insert_header("content-type", "text/event-stream"),
            )
            .expect(1)
            .mount(&server)
            .await;

        let provider = OpenAIProvider::new(
            Client::new(),
            None,
            Some(server.uri()),
            "test-model",
            None,
        );

        let ctx = RequestContext { priority: RequestContext::PRIORITY_VIP };
        let result = provider.chat_stream(minimal_chat_request(), &ctx).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn chat_includes_bearer_auth_alongside_priority() {
        let server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path("/chat/completions"))
            .and(header("Authorization", "Bearer sk-test-key"))
            .and(header("X-Gateway-Priority", "100"))
            .respond_with(ResponseTemplate::new(200).set_body_json(fake_completion_response()))
            .expect(1)
            .mount(&server)
            .await;

        let provider = OpenAIProvider::new(
            Client::new(),
            Some("sk-test-key".to_string()),
            Some(server.uri()),
            "test-model",
            None,
        );

        let ctx = RequestContext { priority: 100 };
        let result = provider.chat(minimal_chat_request(), &ctx).await;
        assert!(result.is_ok());
    }
}

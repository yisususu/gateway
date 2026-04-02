use crate::types::*;
use std::collections::HashMap;

// ============================================================
// Request Conversion: Anthropic → OpenAI
// ============================================================

/// Convert an Anthropic Messages API request to OpenAI ChatCompletion format.
pub fn anthropic_request_to_openai(req: &AnthropicMessagesRequest) -> ChatCompletionRequest {
    let mut messages = Vec::new();

    // 1. System prompt → System role message.
    if let Some(ref system) = req.system {
        let text = extract_system_text(system);
        if !text.is_empty() {
            messages.push(Message {
                role: MessageRole::System,
                content: MessageContent::Text(text),
                name: None,
                tool_calls: None,
                tool_call_id: None,
            });
        }
    }

    // 2. Convert messages.
    for msg in &req.messages {
        match msg.role.as_str() {
            "user" => convert_user_message(&msg.content, &mut messages),
            "assistant" => convert_assistant_message(&msg.content, &mut messages),
            _ => {
                // Fallback: treat as user.
                let text = content_to_string(&msg.content);
                messages.push(Message {
                    role: MessageRole::User,
                    content: MessageContent::Text(text),
                    name: None,
                    tool_calls: None,
                    tool_call_id: None,
                });
            }
        }
    }

    // 3. Tools: Anthropic input_schema → OpenAI parameters.
    let tools = req.tools.as_ref().map(|ts| {
        ts.iter()
            .map(|t| Tool {
                tool_type: "function".to_string(),
                function: ToolFunction {
                    name: t.name.clone(),
                    description: t.description.clone(),
                    parameters: t.input_schema.clone(),
                },
            })
            .collect()
    });

    // 4. Stop sequences.
    let stop = req.stop_sequences.as_ref().map(|seqs| {
        if seqs.len() == 1 {
            StopSequence::Single(seqs[0].clone())
        } else {
            StopSequence::Multiple(seqs.clone())
        }
    });

    // 5. Extra fields (thinking, metadata, etc.).
    let mut extra = serde_json::Map::new();
    if let Some(ref thinking) = req.thinking {
        extra.insert("thinking".to_string(), thinking.clone());
    }
    if let Some(ref metadata) = req.metadata {
        extra.insert("metadata".to_string(), metadata.clone());
    }
    for (k, v) in &req.extra {
        extra.insert(k.clone(), v.clone());
    }

    ChatCompletionRequest {
        model: req.model.clone(),
        messages,
        temperature: req.temperature,
        top_p: req.top_p,
        max_tokens: req.max_tokens,
        max_completion_tokens: None,
        stream: req.stream,
        stop,
        n: None,
        tools,
        tool_choice: req.tool_choice.clone(),
        response_format: None,
        extra,
    }
}

// ============================================================
// Response Conversion: OpenAI → Anthropic
// ============================================================

/// Convert an OpenAI ChatCompletion response to Anthropic Messages format.
pub fn openai_response_to_anthropic(resp: &ChatCompletionResponse) -> AnthropicMessagesResponse {
    let mut content_blocks = Vec::new();

    if let Some(choice) = resp.choices.first() {
        // Text content.
        let text = match &choice.message.content {
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
        if !text.is_empty() {
            content_blocks.push(AnthropicResponseContentBlock::Text { text });
        }

        // Tool calls.
        if let Some(ref tool_calls) = choice.message.tool_calls {
            for tc in tool_calls {
                let input: serde_json::Value =
                    serde_json::from_str(&tc.function.arguments).unwrap_or(serde_json::Value::Null);
                content_blocks.push(AnthropicResponseContentBlock::ToolUse {
                    id: tc.id.clone(),
                    name: tc.function.name.clone(),
                    input,
                });
            }
        }
    }

    if content_blocks.is_empty() {
        content_blocks.push(AnthropicResponseContentBlock::Text {
            text: String::new(),
        });
    }

    let stop_reason = resp
        .choices
        .first()
        .and_then(|c| finish_reason_to_stop_reason(&c.finish_reason));

    AnthropicMessagesResponse {
        id: generate_anthropic_message_id(),
        response_type: "message".to_string(),
        role: "assistant".to_string(),
        content: content_blocks,
        model: resp.model.clone(),
        stop_reason,
        stop_sequence: None,
        usage: AnthropicUsage {
            input_tokens: resp.usage.prompt_tokens,
            output_tokens: resp.usage.completion_tokens,
            cache_creation_input_tokens: None,
            cache_read_input_tokens: None,
        },
    }
}

// ============================================================
// Stream Transcoder: OpenAI chunks → Anthropic SSE events
// ============================================================

/// A single Anthropic SSE event (event type + JSON data).
pub struct AnthropicSseEvent {
    pub event: String,
    pub data: String,
}

/// Stateful transcoder that converts OpenAI stream chunks into Anthropic SSE events.
pub struct AnthropicStreamTranscoder {
    response_id: String,
    model: String,
    message_started: bool,
    content_block_index: u32,
    text_block_open: bool,
    /// Maps OpenAI tool_call index → Anthropic content block index.
    tool_block_map: HashMap<u32, u32>,
    output_tokens: u32,
}

impl AnthropicStreamTranscoder {
    pub fn new(model: String) -> Self {
        Self {
            response_id: generate_anthropic_message_id(),
            model,
            message_started: false,
            content_block_index: 0,
            text_block_open: false,
            tool_block_map: HashMap::new(),
            output_tokens: 0,
        }
    }

    /// Convert one OpenAI stream chunk into zero or more Anthropic SSE events.
    pub fn transcode(&mut self, chunk: &ChatStreamChunk) -> Vec<AnthropicSseEvent> {
        let mut events = Vec::new();

        for choice in &chunk.choices {
            // First chunk: emit message_start.
            if !self.message_started {
                self.message_started = true;
                events.push(AnthropicSseEvent {
                    event: "message_start".to_string(),
                    data: serde_json::json!({
                        "type": "message_start",
                        "message": {
                            "id": self.response_id,
                            "type": "message",
                            "role": "assistant",
                            "content": [],
                            "model": self.model,
                            "stop_reason": null,
                            "stop_sequence": null,
                            "usage": { "input_tokens": 0, "output_tokens": 0 }
                        }
                    })
                    .to_string(),
                });
            }

            // Text content delta.
            if let Some(ref text) = choice.delta.content {
                if !self.text_block_open {
                    self.text_block_open = true;
                    let idx = self.content_block_index;
                    events.push(AnthropicSseEvent {
                        event: "content_block_start".to_string(),
                        data: serde_json::json!({
                            "type": "content_block_start",
                            "index": idx,
                            "content_block": { "type": "text", "text": "" }
                        })
                        .to_string(),
                    });
                }
                if !text.is_empty() {
                    self.output_tokens += 1;
                    events.push(AnthropicSseEvent {
                        event: "content_block_delta".to_string(),
                        data: serde_json::json!({
                            "type": "content_block_delta",
                            "index": self.content_block_index,
                            "delta": { "type": "text_delta", "text": text }
                        })
                        .to_string(),
                    });
                }
            }

            // Tool call deltas.
            if let Some(ref tool_calls) = choice.delta.tool_calls {
                // Close text block if open before starting tool blocks.
                if self.text_block_open {
                    self.text_block_open = false;
                    let idx = self.content_block_index;
                    self.content_block_index += 1;
                    events.push(AnthropicSseEvent {
                        event: "content_block_stop".to_string(),
                        data: serde_json::json!({ "type": "content_block_stop", "index": idx })
                            .to_string(),
                    });
                }

                for tc in tool_calls {
                    // New tool call: has id.
                    if let Some(ref id) = tc.id {
                        let name = tc
                            .function
                            .as_ref()
                            .and_then(|f| f.name.clone())
                            .unwrap_or_default();
                        let content_idx = self.content_block_index;
                        self.tool_block_map.insert(tc.index, content_idx);
                        self.content_block_index += 1;
                        events.push(AnthropicSseEvent {
                            event: "content_block_start".to_string(),
                            data: serde_json::json!({
                                "type": "content_block_start",
                                "index": content_idx,
                                "content_block": {
                                    "type": "tool_use",
                                    "id": id,
                                    "name": name,
                                    "input": {}
                                }
                            })
                            .to_string(),
                        });
                    }
                    // Argument delta.
                    if let Some(ref func) = tc.function {
                        if let Some(ref args) = func.arguments {
                            if !args.is_empty() {
                                let content_idx =
                                    self.tool_block_map.get(&tc.index).copied().unwrap_or(0);
                                events.push(AnthropicSseEvent {
                                    event: "content_block_delta".to_string(),
                                    data: serde_json::json!({
                                        "type": "content_block_delta",
                                        "index": content_idx,
                                        "delta": {
                                            "type": "input_json_delta",
                                            "partial_json": args
                                        }
                                    })
                                    .to_string(),
                                });
                            }
                        }
                    }
                }
            }

            // Finish.
            if choice.finish_reason.is_some() {
                // Close text block.
                if self.text_block_open {
                    self.text_block_open = false;
                    let idx = self.content_block_index;
                    self.content_block_index += 1;
                    events.push(AnthropicSseEvent {
                        event: "content_block_stop".to_string(),
                        data: serde_json::json!({ "type": "content_block_stop", "index": idx })
                            .to_string(),
                    });
                }
                // Close tool blocks.
                for &idx in self.tool_block_map.values() {
                    events.push(AnthropicSseEvent {
                        event: "content_block_stop".to_string(),
                        data: serde_json::json!({ "type": "content_block_stop", "index": idx })
                            .to_string(),
                    });
                }
                self.tool_block_map.clear();

                let stop_reason = finish_reason_to_stop_reason(&choice.finish_reason);
                events.push(AnthropicSseEvent {
                    event: "message_delta".to_string(),
                    data: serde_json::json!({
                        "type": "message_delta",
                        "delta": {
                            "stop_reason": stop_reason,
                            "stop_sequence": null
                        },
                        "usage": { "output_tokens": self.output_tokens.max(1) }
                    })
                    .to_string(),
                });
                events.push(AnthropicSseEvent {
                    event: "message_stop".to_string(),
                    data: "{\"type\":\"message_stop\"}".to_string(),
                });
            }
        }

        events
    }
}

// ============================================================
// Internal Helpers
// ============================================================

fn extract_system_text(system: &AnthropicSystemContent) -> String {
    match system {
        AnthropicSystemContent::Text(t) => t.clone(),
        AnthropicSystemContent::Blocks(blocks) => blocks
            .iter()
            .filter_map(|b| {
                if b.block_type == "text" {
                    Some(b.text.as_str())
                } else {
                    None
                }
            })
            .collect::<Vec<_>>()
            .join("\n"),
    }
}

fn content_to_string(content: &AnthropicContent) -> String {
    match content {
        AnthropicContent::Text(t) => t.clone(),
        AnthropicContent::Blocks(blocks) => blocks
            .iter()
            .filter_map(|b| match b {
                AnthropicContentBlock::Text { text } => Some(text.as_str()),
                _ => None,
            })
            .collect::<Vec<_>>()
            .join(""),
    }
}

/// Convert user message (may contain tool_result blocks → multiple OpenAI messages).
fn convert_user_message(content: &AnthropicContent, messages: &mut Vec<Message>) {
    match content {
        AnthropicContent::Text(t) => {
            messages.push(Message {
                role: MessageRole::User,
                content: MessageContent::Text(t.clone()),
                name: None,
                tool_calls: None,
                tool_call_id: None,
            });
        }
        AnthropicContent::Blocks(blocks) => {
            let mut text_parts = Vec::new();
            let mut tool_results = Vec::new();

            for block in blocks {
                match block {
                    AnthropicContentBlock::Text { text } => {
                        text_parts.push(ContentPart::Text { text: text.clone() });
                    }
                    AnthropicContentBlock::Image { source } => {
                        text_parts.push(ContentPart::ImageUrl {
                            image_url: ImageUrl {
                                url: serde_json::to_string(source).unwrap_or_default(),
                                detail: None,
                            },
                        });
                    }
                    AnthropicContentBlock::ToolResult {
                        tool_use_id,
                        content: result_content,
                        ..
                    } => {
                        let text = match result_content {
                            Some(AnthropicContent::Text(t)) => t.clone(),
                            Some(AnthropicContent::Blocks(bs)) => bs
                                .iter()
                                .filter_map(|b| match b {
                                    AnthropicContentBlock::Text { text } => Some(text.as_str()),
                                    _ => None,
                                })
                                .collect::<Vec<_>>()
                                .join(""),
                            None => String::new(),
                        };
                        tool_results.push((tool_use_id.clone(), text));
                    }
                    _ => {}
                }
            }

            // Text/image parts → User message.
            if !text_parts.is_empty() {
                messages.push(Message {
                    role: MessageRole::User,
                    content: MessageContent::Parts(text_parts),
                    name: None,
                    tool_calls: None,
                    tool_call_id: None,
                });
            }

            // Each tool_result → separate Tool role message.
            for (tool_use_id, text) in tool_results {
                messages.push(Message {
                    role: MessageRole::Tool,
                    content: MessageContent::Text(text),
                    name: None,
                    tool_calls: None,
                    tool_call_id: Some(tool_use_id),
                });
            }
        }
    }
}

/// Convert assistant message (may contain tool_use blocks → tool_calls).
fn convert_assistant_message(content: &AnthropicContent, messages: &mut Vec<Message>) {
    match content {
        AnthropicContent::Text(t) => {
            messages.push(Message {
                role: MessageRole::Assistant,
                content: MessageContent::Text(t.clone()),
                name: None,
                tool_calls: None,
                tool_call_id: None,
            });
        }
        AnthropicContent::Blocks(blocks) => {
            let mut text_parts = Vec::new();
            let mut tool_calls = Vec::new();

            for block in blocks {
                match block {
                    AnthropicContentBlock::Text { text } => {
                        text_parts.push(text.as_str());
                    }
                    AnthropicContentBlock::ToolUse { id, name, input } => {
                        tool_calls.push(ToolCall {
                            id: id.clone(),
                            call_type: "function".to_string(),
                            function: FunctionCall {
                                name: name.clone(),
                                arguments: serde_json::to_string(input).unwrap_or_default(),
                            },
                        });
                    }
                    _ => {}
                }
            }

            let text = text_parts.join("");
            messages.push(Message {
                role: MessageRole::Assistant,
                content: MessageContent::Text(text),
                name: None,
                tool_calls: if tool_calls.is_empty() {
                    None
                } else {
                    Some(tool_calls)
                },
                tool_call_id: None,
            });
        }
    }
}

/// Map OpenAI finish_reason to Anthropic stop_reason.
fn finish_reason_to_stop_reason(reason: &Option<String>) -> Option<String> {
    reason.as_ref().map(|r| match r.as_str() {
        "stop" => "end_turn",
        "tool_calls" => "tool_use",
        "length" => "max_tokens",
        other => other,
    }.to_string())
}

/// Generate a `msg_` prefixed random ID.
fn generate_anthropic_message_id() -> String {
    let id = uuid::Uuid::new_v4();
    format!("msg_{}", id.simple())
}

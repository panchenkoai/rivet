//! Operator notifications — Slack webhooks, on-event triggers.

use schemars::JsonSchema;
use serde::Deserialize;

#[derive(Debug, Deserialize, JsonSchema, Clone)]
#[serde(deny_unknown_fields)]
pub struct NotificationsConfig {
    pub slack: Option<SlackConfig>,
}

#[derive(Debug, Deserialize, JsonSchema, Clone)]
#[serde(deny_unknown_fields)]
pub struct SlackConfig {
    pub webhook_url: Option<String>,
    pub webhook_url_env: Option<String>,
    #[serde(default)]
    pub on: Vec<NotifyEvent>,
}

#[derive(Debug, Deserialize, JsonSchema, Clone, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum NotifyEvent {
    Failure,
    SchemaChange,
    Degraded,
}

use crate::config::{NotificationsConfig, NotifyEvent};
use crate::pipeline::RunSummary;

pub fn maybe_send(config: Option<&NotificationsConfig>, summary: &RunSummary) {
    let Some(cfg) = config else { return };
    let Some(slack) = &cfg.slack else { return };

    let triggers = &slack.on;
    let should_send = triggers.iter().any(|t| match t {
        NotifyEvent::Failure => summary.status == "failed",
        NotifyEvent::SchemaChange => summary.schema_changed == Some(true),
        NotifyEvent::Degraded => summary.status == "degraded",
    });

    if !should_send {
        return;
    }

    let url = match (&slack.webhook_url, &slack.webhook_url_env) {
        (Some(u), _) => u.clone(),
        (None, Some(env)) => match std::env::var(env) {
            Ok(u) => u,
            Err(_) => {
                log::warn!("slack notification skipped: env var '{}' not set", env);
                return;
            }
        },
        (None, None) => {
            log::warn!("slack notification skipped: no webhook_url or webhook_url_env configured");
            return;
        }
    };

    let color = if summary.status == "failed" {
        "#e74c3c"
    } else {
        "#f39c12"
    };
    let text = format!(
        "*{}* | status: `{}` | rows: {} | duration: {}ms{}{}",
        summary.export_name,
        summary.status,
        summary.total_rows,
        summary.duration_ms,
        summary
            .error_message
            .as_ref()
            .map(|e| format!("\nerror: {}", e))
            .unwrap_or_default(),
        if summary.schema_changed == Some(true) {
            "\nschema changed"
        } else {
            ""
        },
    );

    let payload = serde_json::json!({
        "attachments": [{
            "color": color,
            "title": format!("Rivet: {}", summary.run_id),
            "text": text,
            "footer": "rivet export notification",
        }]
    });

    match reqwest::blocking::Client::builder()
        .timeout(std::time::Duration::from_secs(10))
        .build()
        .and_then(|c| c.post(&url).json(&payload).send())
    {
        Ok(resp) if resp.status().is_success() => {
            log::info!("slack notification sent for '{}'", summary.export_name);
        }
        Ok(resp) => {
            log::warn!("slack notification failed: HTTP {}", resp.status());
        }
        Err(e) => {
            log::warn!("slack notification failed: {}", e);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{NotificationsConfig, NotifyEvent, SlackConfig};

    fn stub_summary(status: &str, schema_changed: Option<bool>) -> RunSummary {
        RunSummary {
            run_id: "test_run".into(),
            export_name: "test_export".into(),
            status: status.into(),
            total_rows: 100,
            files_produced: 1,
            bytes_written: 1024,
            duration_ms: 500,
            peak_rss_mb: 10,
            retries: 0,
            validated: None,
            schema_changed,
            quality_passed: None,
            error_message: None,
            tuning_profile: "balanced".into(),
            batch_size: 10_000,
            batch_size_memory_mb: None,
            format: "parquet".into(),
            mode: "full".into(),
            compression: "zstd".into(),
            source_count: None,
            reconciled: None,
        }
    }

    #[test]
    fn no_config_does_nothing() {
        maybe_send(None, &stub_summary("success", None));
    }

    #[test]
    fn no_slack_does_nothing() {
        let cfg = NotificationsConfig { slack: None };
        maybe_send(Some(&cfg), &stub_summary("failed", None));
    }

    #[test]
    fn success_does_not_trigger_failure() {
        let cfg = NotificationsConfig {
            slack: Some(SlackConfig {
                webhook_url: Some("http://localhost:1/noop".into()),
                webhook_url_env: None,
                on: vec![NotifyEvent::Failure],
            }),
        };
        // should_send = false, so no HTTP call
        maybe_send(Some(&cfg), &stub_summary("success", None));
    }

    #[test]
    fn degraded_triggers_degraded_event() {
        let cfg = NotificationsConfig {
            slack: Some(SlackConfig {
                webhook_url: Some("http://127.0.0.1:1/noop".into()),
                webhook_url_env: None,
                on: vec![NotifyEvent::Degraded],
            }),
        };
        // "degraded" status should match; the HTTP will fail silently (port 1)
        maybe_send(Some(&cfg), &stub_summary("degraded", None));
    }

    #[test]
    fn schema_change_triggers_schema_change_event() {
        let cfg = NotificationsConfig {
            slack: Some(SlackConfig {
                webhook_url: Some("http://127.0.0.1:1/noop".into()),
                webhook_url_env: None,
                on: vec![NotifyEvent::SchemaChange],
            }),
        };
        maybe_send(Some(&cfg), &stub_summary("success", Some(true)));
    }

    #[test]
    fn schema_change_false_does_not_trigger() {
        let cfg = NotificationsConfig {
            slack: Some(SlackConfig {
                webhook_url: Some("http://127.0.0.1:1/noop".into()),
                webhook_url_env: None,
                on: vec![NotifyEvent::SchemaChange],
            }),
        };
        // schema_changed = Some(false) → should_send = false
        maybe_send(Some(&cfg), &stub_summary("success", Some(false)));
    }

    #[test]
    fn missing_webhook_url_env_skips_silently() {
        unsafe {
            std::env::remove_var("RIVET_TEST_SLACK_NONEXISTENT");
        }
        let cfg = NotificationsConfig {
            slack: Some(SlackConfig {
                webhook_url: None,
                webhook_url_env: Some("RIVET_TEST_SLACK_NONEXISTENT".into()),
                on: vec![NotifyEvent::Failure],
            }),
        };
        // should_send = true, but env var missing → skips
        maybe_send(Some(&cfg), &stub_summary("failed", None));
    }

    #[test]
    fn no_webhook_url_and_no_env_skips() {
        let cfg = NotificationsConfig {
            slack: Some(SlackConfig {
                webhook_url: None,
                webhook_url_env: None,
                on: vec![NotifyEvent::Failure],
            }),
        };
        maybe_send(Some(&cfg), &stub_summary("failed", None));
    }

    #[test]
    fn multiple_triggers_any_match_fires() {
        let cfg = NotificationsConfig {
            slack: Some(SlackConfig {
                webhook_url: Some("http://127.0.0.1:1/noop".into()),
                webhook_url_env: None,
                on: vec![
                    NotifyEvent::Failure,
                    NotifyEvent::SchemaChange,
                    NotifyEvent::Degraded,
                ],
            }),
        };
        // "degraded" matches the 3rd trigger
        maybe_send(Some(&cfg), &stub_summary("degraded", None));
    }

    #[test]
    fn error_message_included_in_stub() {
        let mut s = stub_summary("failed", None);
        s.error_message = Some("connection refused".into());
        let cfg = NotificationsConfig {
            slack: Some(SlackConfig {
                webhook_url: Some("http://127.0.0.1:1/noop".into()),
                webhook_url_env: None,
                on: vec![NotifyEvent::Failure],
            }),
        };
        maybe_send(Some(&cfg), &s);
    }
}

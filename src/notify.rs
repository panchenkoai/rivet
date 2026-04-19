use crate::config::{NotificationsConfig, NotifyEvent};
use crate::pipeline::RunSummary;

/// Build the JSON payload sent to a Slack-compatible webhook.
///
/// Pure function (no I/O, no global state): the payload is a deterministic
/// function of the `RunSummary`.  Extracted from `maybe_send` so contract
/// tests can pin the shape of the payload without spinning up an HTTP
/// receiver or the blocking `reqwest` client.
pub(crate) fn build_slack_payload(summary: &RunSummary) -> serde_json::Value {
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

    serde_json::json!({
        "attachments": [{
            "color": color,
            "title": format!("Rivet: {}", summary.run_id),
            "text": text,
            "footer": "rivet export notification",
        }]
    })
}

/// Evaluate whether the configured triggers fire for this summary.
///
/// Exposed to tests so the trigger truth-table can be exercised without
/// touching the network path.
pub(crate) fn should_notify(triggers: &[NotifyEvent], summary: &RunSummary) -> bool {
    triggers.iter().any(|t| match t {
        NotifyEvent::Failure => summary.status == "failed",
        NotifyEvent::SchemaChange => summary.schema_changed == Some(true),
        NotifyEvent::Degraded => summary.status == "degraded",
    })
}

pub fn maybe_send(config: Option<&NotificationsConfig>, summary: &RunSummary) {
    let Some(cfg) = config else { return };
    let Some(slack) = &cfg.slack else { return };

    if !should_notify(&slack.on, summary) {
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

    let payload = build_slack_payload(summary);

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
            journal: crate::pipeline::journal::RunJournal::new("test_run", "test_export"),
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

    // ─── Webhook payload & mock receiver (QA backlog Task 8.2) ──────────────

    /// Build a summary tagged with the given marker in `error_message` so
    /// redaction assertions can locate secrets in the JSON output.
    fn failed_summary_with_error(marker: &str) -> RunSummary {
        let mut s = stub_summary("failed", None);
        s.error_message = Some(marker.into());
        s
    }

    #[test]
    fn build_slack_payload_failure_has_required_fields_and_red_color() {
        let s = stub_summary("failed", None);
        let payload = build_slack_payload(&s);
        let att = &payload["attachments"][0];
        assert_eq!(att["color"], "#e74c3c", "failed runs must use red color");
        assert_eq!(att["title"], format!("Rivet: {}", s.run_id));
        let text = att["text"].as_str().expect("text is always a string");
        assert!(text.contains(&s.export_name));
        assert!(text.contains("status: `failed`"));
        assert!(text.contains("rows: 100"));
        assert!(text.contains("duration: 500ms"));
    }

    #[test]
    fn build_slack_payload_schema_change_text_is_appended() {
        let s = stub_summary("success", Some(true));
        let payload = build_slack_payload(&s);
        let text = payload["attachments"][0]["text"].as_str().unwrap();
        assert!(
            text.contains("schema changed"),
            "schema change must surface in the notification text; got: {text}"
        );
    }

    #[test]
    fn build_slack_payload_schema_unchanged_does_not_add_schema_line() {
        for schema in [None, Some(false)] {
            let s = stub_summary("success", schema);
            let payload = build_slack_payload(&s);
            let text = payload["attachments"][0]["text"].as_str().unwrap();
            assert!(
                !text.contains("schema changed"),
                "schema text must only appear when schema_changed == Some(true); got: {text}"
            );
        }
    }

    #[test]
    fn build_slack_payload_error_message_is_included_verbatim() {
        let s = failed_summary_with_error("connection refused to db-1");
        let payload = build_slack_payload(&s);
        let text = payload["attachments"][0]["text"].as_str().unwrap();
        assert!(
            text.contains("error: connection refused to db-1"),
            "error_message must be included in the text: {text}"
        );
    }

    /// Secret redaction is a pipeline/state-layer responsibility: if a secret
    /// leaks into `summary.error_message`, `build_slack_payload` will echo it.
    /// This test pins the contract so a future well-meaning refactor cannot
    /// silently *start* echoing `summary.source.password` or similar fields.
    ///
    /// Today payload has exactly two sources of user-controlled text:
    ///   - `summary.export_name`
    ///   - `summary.error_message`
    ///
    /// None of the other `RunSummary` fields that could ever carry secrets
    /// (`tuning_profile`, `format`, `mode`, `compression`) are allowed into
    /// the payload.
    #[test]
    fn build_slack_payload_does_not_pull_in_fields_beyond_documented_set() {
        let mut s = stub_summary("failed", None);
        // Fill every other field with a distinct marker; payload text must
        // mention none of them.
        s.tuning_profile = "MARK_tuning_profile_MUST_NOT_LEAK".into();
        s.format = "MARK_format".into();
        s.mode = "MARK_mode".into();
        s.compression = "MARK_compression".into();

        let payload = build_slack_payload(&s);
        let text = payload["attachments"][0]["text"].as_str().unwrap();
        for marker in [
            "MARK_tuning_profile_MUST_NOT_LEAK",
            "MARK_format",
            "MARK_mode",
            "MARK_compression",
        ] {
            assert!(
                !text.contains(marker),
                "field marker '{marker}' must not appear in notification text: {text}"
            );
        }
    }

    #[test]
    fn should_notify_truth_table() {
        let success_no_schema = stub_summary("success", None);
        let failed = stub_summary("failed", None);
        let degraded = stub_summary("degraded", None);
        let success_schema_change = stub_summary("success", Some(true));

        // Empty trigger list → never notify.
        assert!(!should_notify(&[], &failed));

        // Failure trigger fires on failed only.
        let failure_trigger = [NotifyEvent::Failure];
        assert!(should_notify(&failure_trigger, &failed));
        assert!(!should_notify(&failure_trigger, &success_no_schema));
        assert!(!should_notify(&failure_trigger, &degraded));

        // Degraded trigger fires on degraded only.
        let degraded_trigger = [NotifyEvent::Degraded];
        assert!(should_notify(&degraded_trigger, &degraded));
        assert!(!should_notify(&degraded_trigger, &failed));

        // Schema-change trigger ignores status, checks schema_changed == Some(true).
        let schema_trigger = [NotifyEvent::SchemaChange];
        assert!(should_notify(&schema_trigger, &success_schema_change));
        assert!(!should_notify(&schema_trigger, &success_no_schema));
        assert!(
            !should_notify(&schema_trigger, &stub_summary("success", Some(false))),
            "Some(false) must not trigger — it is an explicit 'no change'"
        );
    }

    // ─── Mock HTTP receiver ─────────────────────────────────────────────────

    /// A minimal TCP-based HTTP/1.1 "server" that accepts one connection,
    /// reads the request, and replies with a configurable status line.
    ///
    /// Returns `(url, join_handle)` where:
    ///   * `url` is a `http://127.0.0.1:<port>` that can be placed into
    ///     `SlackConfig::webhook_url`;
    ///   * `join_handle` lets the test wait for the response to be sent
    ///     (and collect the captured request body for assertions).
    fn mock_webhook(status_line: &'static str) -> (String, std::thread::JoinHandle<String>) {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let url = format!("http://{}/hook", addr);

        let handle = std::thread::spawn(move || {
            use std::io::{Read, Write};
            let (mut stream, _) = listener.accept().expect("mock webhook: accept");
            // Read up to 64 KiB of the request — enough for any realistic
            // notification payload.
            let mut buf = [0u8; 64 * 1024];
            let n = stream.read(&mut buf).unwrap_or(0);
            let request = String::from_utf8_lossy(&buf[..n]).into_owned();

            let body = "ok";
            let response = format!(
                "HTTP/1.1 {}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                status_line,
                body.len(),
                body,
            );
            let _ = stream.write_all(response.as_bytes());
            let _ = stream.flush();
            request
        });

        (url, handle)
    }

    #[test]
    fn maybe_send_posts_to_webhook_when_failure_trigger_fires() {
        let (url, handle) = mock_webhook("200 OK");
        let cfg = NotificationsConfig {
            slack: Some(SlackConfig {
                webhook_url: Some(url),
                webhook_url_env: None,
                on: vec![NotifyEvent::Failure],
            }),
        };
        maybe_send(Some(&cfg), &stub_summary("failed", None));

        let request = handle.join().expect("mock receiver panicked");
        assert!(
            request.starts_with("POST /hook"),
            "maybe_send must issue a POST to the webhook path; got request:\n{request}"
        );
        assert!(
            request.contains("application/json"),
            "Content-Type must be JSON; got:\n{request}"
        );
        assert!(
            request.contains("\"attachments\""),
            "payload must include Slack-style attachments block; got:\n{request}"
        );
    }

    #[test]
    fn maybe_send_swallows_5xx_response_without_panic() {
        let (url, handle) = mock_webhook("500 Internal Server Error");
        let cfg = NotificationsConfig {
            slack: Some(SlackConfig {
                webhook_url: Some(url),
                webhook_url_env: None,
                on: vec![NotifyEvent::Failure],
            }),
        };
        // Must not panic; the 5xx is logged and the pipeline continues.
        maybe_send(Some(&cfg), &stub_summary("failed", None));
        let _ = handle.join();
    }

    #[test]
    fn maybe_send_swallows_429_response_without_panic() {
        let (url, handle) = mock_webhook("429 Too Many Requests");
        let cfg = NotificationsConfig {
            slack: Some(SlackConfig {
                webhook_url: Some(url),
                webhook_url_env: None,
                on: vec![NotifyEvent::Failure],
            }),
        };
        maybe_send(Some(&cfg), &stub_summary("failed", None));
        let _ = handle.join();
    }

    #[test]
    fn maybe_send_does_not_fire_when_no_trigger_matches() {
        // Start a receiver and immediately assert it is NOT contacted — the
        // join handle times out via a very short wait after maybe_send returns.
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let url = format!("http://{}/hook", addr);
        listener
            .set_nonblocking(true)
            .expect("set_nonblocking supported");

        let cfg = NotificationsConfig {
            slack: Some(SlackConfig {
                webhook_url: Some(url),
                webhook_url_env: None,
                on: vec![NotifyEvent::Failure],
            }),
        };
        // Success must not trigger the Failure listener.
        maybe_send(Some(&cfg), &stub_summary("success", None));

        // No connection should have been made; `accept()` must return
        // `WouldBlock` on a non-blocking listener.
        match listener.accept() {
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {}
            Ok(_) => panic!("maybe_send contacted the webhook even though no trigger matched"),
            Err(e) => panic!("unexpected listener error: {e}"),
        }
    }
}

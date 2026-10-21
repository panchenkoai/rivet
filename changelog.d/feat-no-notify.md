- **A scheduler could not turn rivet's own notifications off.** Through 0.32.1 a config's
  `notifications:` (an inline `webhook_url:` included) fired on every failed run, so a scheduler that
  alerts through its own channel sent each alert twice. **Now** the global flag `--no-notify` sends no
  notification from the process, and is passed on to every process child `--parallel-export-processes`
  starts (ADR-0039 D9). Every global flag is now either passed on to the children or declared
  parent-only, checked against the clap definition. **What to do:** nothing; a scheduler integration
  passes `--no-notify`.

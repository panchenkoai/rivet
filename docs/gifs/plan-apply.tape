# Rivet — plan/apply: sealed execution artifact for CI/CD.
# Rendered by docs/gifs/render.sh.

Output plan-apply.gif

Require bash
Set Shell bash

Set FontSize 14
Set Width 1200
Set Height 720
Set Padding 20
Set TypingSpeed 35ms
Set PlaybackSpeed 1
Set Theme "Dracula"

Hide
Type `export PATH=$RIVET_BIN_DIR:/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin`
Enter
Type `export PS1='rivet-demo $ '`
Enter
Type "rivet init --source-env DATABASE_URL --table orders -o orders.yaml >/dev/null 2>&1"
Enter
Sleep 1500ms
Type "clear"
Enter
Sleep 500ms
Show

Type "# Step 1 — seal the intent into an auditable plan artifact"
Enter
Sleep 1500ms

Type "rivet plan --config orders.yaml --format json --output plan.json"
Enter
Sleep 2500ms

Type "ls -lh plan.json"
Enter
Sleep 2s

Type "# No plaintext secrets — ADR-0005 PA9 redacts password/userinfo"
Enter
Sleep 1500ms

Type `jq '.resolved_plan.source | {type, url_env, password}' plan.json`
Enter
Sleep 2500ms

Type "# Step 2 — execute the sealed plan. No config re-read, no preflight."
Enter
Sleep 1500ms

Type "rivet apply plan.json"
Enter
Sleep 3s

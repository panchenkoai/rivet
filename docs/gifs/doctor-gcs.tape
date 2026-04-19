# Rivet — rivet doctor + rivet run against real GCS via Application Default
# Credentials. Used in docs/destinations/gcs.md to show the end-to-end auth
# flow without any credential file on disk (ADC is resolved via gcloud auth
# application-default login).

Output doctor-gcs.gif

Require bash
Set Shell bash

Set FontSize 14
Set Width 1280
Set Height 700
Set Padding 20
Set TypingSpeed 30ms
Set PlaybackSpeed 1
Set Theme "Dracula"

Hide
Type `export PATH=$RIVET_BIN_DIR:$(dirname $PSQL_BIN):$(dirname $GCLOUD_BIN):/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin`
Enter
# Force ADC discovery via ~/.config/gcloud/application_default_credentials.json
# even if the parent shell exported GOOGLE_APPLICATION_CREDENTIALS.
Type "unset GOOGLE_APPLICATION_CREDENTIALS"
Enter
# Silence "Updates are available" / survey chatter so the final frame stays
# focused on doctor + run + gcloud ls output.
Type "export CLOUDSDK_CORE_DISABLE_USAGE_REPORTING=true CLOUDSDK_COMPONENT_MANAGER_DISABLE_UPDATE_CHECK=true CLOUDSDK_SURVEY_DISABLE_PROMPTS=true"
Enter
Type `export PS1='rivet-demo $ '`
Enter
Type "clear"
Enter
Sleep 300ms
Show

Type "# YAML: real GCS bucket, auth resolved via Application Default Credentials"
Enter
Sleep 1500ms

Type "cat gcs.yaml"
Enter
Sleep 3500ms

Type "rivet doctor --config gcs.yaml"
Enter
Sleep 4s

Type "rivet run --config gcs.yaml --validate"
Enter
Sleep 5s

Type "gcloud storage ls gs://rivet_data_test/rivet-gif-demo/"
Enter
Sleep 3s

# Airflow worker image with the rivet binary baked in, so the DAG's tasks can run
# `rivet run` for real. Pulls the published linux release for the build arch.
#
#   docker compose -f docker-compose.2.10.yaml build
#
# Override the versions via build args (or the compose `build.args:` block).
ARG AIRFLOW_VERSION=2.10.5
FROM apache/airflow:${AIRFLOW_VERSION}

USER root
ARG RIVET_VERSION=0.16.3
ARG TARGETARCH
RUN set -eux; \
    case "${TARGETARCH:-arm64}" in \
      amd64) rarch=x86_64-unknown-linux-gnu ;; \
      arm64) rarch=aarch64-unknown-linux-gnu ;; \
      *) echo "unsupported arch: ${TARGETARCH}" >&2; exit 1 ;; \
    esac; \
    curl -fsSL "https://github.com/panchenkoai/rivet/releases/download/v${RIVET_VERSION}/rivet-v${RIVET_VERSION}-${rarch}.tar.gz" \
      | tar -xz -C /usr/local/bin --strip-components=1 "rivet-v${RIVET_VERSION}-${rarch}/rivet"; \
    chmod +x /usr/local/bin/rivet; \
    rivet --version

USER airflow

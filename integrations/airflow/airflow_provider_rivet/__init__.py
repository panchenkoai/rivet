"""Apache Airflow provider that runs the rivet binary on the worker (ADR-0039)."""

from __future__ import annotations

__version__ = "0.1.0"


def get_provider_info() -> dict:
    """Provider metadata Airflow reads through the `apache_airflow_provider` entry point."""
    return {
        "package-name": "airflow-provider-rivet",
        "name": "Rivet",
        "description": "Run rivet exports, CDC drains, loads and compactions as Airflow tasks.",
        "versions": [__version__],
    }

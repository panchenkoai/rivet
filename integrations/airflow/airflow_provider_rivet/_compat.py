"""The names that moved between Airflow 2.10 and 3.x, imported in one place."""

from __future__ import annotations

try:  # Airflow 3.x: the task SDK is the public authoring interface
    from airflow.sdk import DAG, BaseHook, BaseOperator, TaskGroup
except ImportError:  # Airflow 2.10+
    from airflow.hooks.base import BaseHook
    from airflow.models.baseoperator import BaseOperator
    from airflow.models.dag import DAG
    from airflow.utils.task_group import TaskGroup

from airflow.exceptions import AirflowException, AirflowFailException, AirflowSkipException

__all__ = [
    "DAG",
    "AirflowException",
    "AirflowFailException",
    "AirflowSkipException",
    "BaseHook",
    "BaseOperator",
    "TaskGroup",
]

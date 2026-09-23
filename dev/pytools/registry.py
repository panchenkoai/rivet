"""The stand registry (dev/stand/registry.yaml): every name the tests and the gate use."""

from __future__ import annotations

import functools
import os
import re
from pathlib import Path

import yaml

PATH = Path(__file__).resolve().parents[1] / "stand" / "registry.yaml"


@functools.lru_cache(maxsize=1)
def load() -> dict:
    """The parsed registry."""
    return yaml.safe_load(PATH.read_text())


def source(name: str) -> dict:
    """`{container, url}` of one stand source (`postgres`, `mongo_rs`, …)."""
    return load()["sources"][name]


def bq_tmp(name: str) -> str:
    """A disposable BigQuery dataset name — the only kind the sweep may drop."""
    return load()["bigquery"]["tmp_prefix"] + name


def orphaned(name: str) -> bool:
    """Is `name` a test object whose creating process is gone?"""
    m = re.search(load()["test_object_suffix"], name)
    if not m:
        return False
    try:
        os.kill(int(m.group(1)), 0)
    except ProcessLookupError:
        return True
    except (PermissionError, OverflowError, ValueError):
        return False
    return False


def _self_test() -> None:
    assert source("postgres")["container"] == "rivet-postgres-1"
    assert bq_tmp("gate").startswith("rivet_tmp_")
    assert not orphaned("users"), "a persistent fixture is never an orphan"
    assert not orphaned(f"t_{os.getpid()}_3"), "a live process's object is spared"
    dead = 2**22 + 12345
    assert orphaned(f"t_{dead}_1"), "a dead pid's object is swept"
    print("registry self-test ok")


if __name__ == "__main__":
    _self_test()

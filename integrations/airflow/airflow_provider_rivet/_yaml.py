"""Read a rivet config the way rivet does: YAML 1.2 scalars, so `on` and `12:30` stay strings."""

from __future__ import annotations

import re
from typing import Any

import yaml

_CORE = (
    ("tag:yaml.org,2002:bool", re.compile(r"^(?:true|True|TRUE|false|False|FALSE)$"), list("tTfF")),
    ("tag:yaml.org,2002:int", re.compile(r"^[-+]?[0-9]+$"), list("-+0123456789")),
    (
        "tag:yaml.org,2002:float",
        re.compile(r"^[-+]?(?:\.[0-9]+|[0-9]+(?:\.[0-9]*)?)(?:[eE][-+]?[0-9]+)?$"),
        list("-+0123456789."),
    ),
)
_REPLACED = {tag for tag, _, _ in _CORE} | {"tag:yaml.org,2002:timestamp"}


class ConfigLoader(yaml.SafeLoader):
    """SafeLoader with the YAML 1.1 bool / int / float / timestamp guesses replaced by the 1.2 core ones."""


ConfigLoader.yaml_implicit_resolvers = {
    first: [(tag, rx) for tag, rx in pairs if tag not in _REPLACED]
    for first, pairs in yaml.SafeLoader.yaml_implicit_resolvers.items()
}
for _tag, _rx, _first in _CORE:
    ConfigLoader.add_implicit_resolver(_tag, _rx, _first)


def load(text: str) -> Any:
    """Parse config text."""
    return yaml.load(text, Loader=ConfigLoader)  # noqa: S506 - a SafeLoader subclass


def dump(doc: Any) -> str:
    """Write a config back; strings a 1.1 reader would re-type come out quoted."""
    return yaml.safe_dump(doc, sort_keys=False, default_flow_style=False)

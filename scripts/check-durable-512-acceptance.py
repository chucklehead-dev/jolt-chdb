#!/usr/bin/env python3
"""Read the bounded scalar 512-row acceptance receipt without echoing report data."""
from __future__ import annotations

import pathlib
import re
import sys
from decimal import Decimal, InvalidOperation


class InvalidReceipt(Exception):
    pass


def enclosed(source: str, start: int) -> str:
    if source[start] != "{":
        raise InvalidReceipt("expected map")
    stack = []
    quoted = escaped = False
    for offset in range(start, len(source)):
        char = source[offset]
        if quoted:
            if escaped:
                escaped = False
            elif char == "\\":
                escaped = True
            elif char == '"':
                quoted = False
        elif char == '"':
            quoted = True
        elif char in "{[":
            stack.append(char)
        elif char in "}]":
            if not stack or (char == "}" and stack[-1] != "{") or (char == "]" and stack[-1] != "["):
                raise InvalidReceipt("unbalanced collection")
            stack.pop()
            if not stack:
                return source[start:offset + 1]
    raise InvalidReceipt("unclosed collection")


def scalar_map(source: str, keys: tuple[str, ...]) -> dict[str, str]:
    if not (source.startswith("{") and source.endswith("}")):
        raise InvalidReceipt("expected scalar map")
    tokens = source[1:-1].replace(",", " ").split()
    if len(tokens) != len(keys) * 2 or tokens[::2] != list(keys):
        raise InvalidReceipt("unexpected scalar map shape")
    return dict(zip(tokens[::2], tokens[1::2]))


def number(value: str) -> Decimal:
    if not re.fullmatch(r"(?:0|[1-9][0-9]*)(?:\.[0-9]+)?", value):
        raise InvalidReceipt("invalid finite number")
    try:
        return Decimal(value)
    except InvalidOperation as error:
        raise InvalidReceipt("invalid finite number") from error


def receipt(report: pathlib.Path, mode: str) -> str:
    source = report.read_text(encoding="utf-8")
    if not re.match(r"^\{:schema-version 2,?\s+:profile :scale-512(?:,|\s)", source):
        raise InvalidReceipt("unexpected report header")
    if not re.search(r":configurations\s+\[", source) or not re.search(r":phase-log\s+\[", source):
        raise InvalidReceipt("incomplete final report")
    if not re.search(r":configurations\s+\[\{:configuration\s+\{", source):
        raise InvalidReceipt("missing selected configuration")
    if (not re.search(r":selector\s+:scale-512(?:[,}\s])", source)
            or not re.search(r":batch-size\s+512(?:[,}\s])", source)):
        raise InvalidReceipt("unexpected selected configuration")
    if not re.search(r":results\s+\[\{", source) or not re.search(r":summaries\s+\{", source):
        raise InvalidReceipt("missing trial results or summaries")
    if enclosed(source, 0) != source.strip():
        raise InvalidReceipt("trailing report data")
    marker = ":encoding-inclusive-512-acceptance"
    if source.count(marker) != 1:
        raise InvalidReceipt("missing or ambiguous acceptance receipt")
    tail = source.split(marker, 1)[1].lstrip()
    acceptance = enclosed(tail, 0)
    header = re.fullmatch(
        r"\{:status (:[a-z-]+),?(?:\s+:reason (:[a-z-]+),?)?\s+:target (\{[^{}]*\}),?\s+:observed (\{[^{}]*\})\}",
        acceptance,
    )
    if header is None:
        raise InvalidReceipt("unexpected acceptance shape")
    status, reason, target_text, observed_text = header.groups()
    target = scalar_map(target_text, (":sample-count", ":p50-max-ms", ":p99-max-ms"))
    observed = scalar_map(observed_text, (":count", ":p99-qualification?", ":p50-ms", ":p99-ms"))
    if target[":sample-count"] != "500" or observed[":count"] != "500":
        raise InvalidReceipt("incomplete sample count")
    if observed[":p99-qualification?"] != "true":
        raise InvalidReceipt("unqualified p99")
    p50_target, p99_target = number(target[":p50-max-ms"]), number(target[":p99-max-ms"])
    p50, p99 = number(observed[":p50-ms"]), number(observed[":p99-ms"])
    if p50_target != Decimal("20.48") or p99_target != Decimal("25.60"):
        raise InvalidReceipt("unexpected target")
    missed = p50 > p50_target or p99 > p99_target
    if mode == "miss" and (status, reason, missed) != (":failed", ":latency-target-missed", True):
        raise InvalidReceipt("inconsistent target miss")
    if mode == "pass" and ((status, reason, missed) != (":passed", None, False)):
        raise InvalidReceipt("inconsistent target pass")
    return (f"p50={p50}ms target={p50_target}ms "
            f"p99={p99}ms target={p99_target}ms samples=500")


if __name__ == "__main__":
    try:
        if len(sys.argv) != 3 or sys.argv[2] not in ("pass", "miss"):
            raise InvalidReceipt("usage: check-durable-512-acceptance.py REPORT pass|miss")
        result = receipt(pathlib.Path(sys.argv[1]), sys.argv[2])
    except (InvalidReceipt, OSError, UnicodeError) as error:
        print(f"Durable 512 acceptance receipt invalid: {error}", file=sys.stderr)
        sys.exit(1)
    if sys.argv[2] == "miss":
        print(f"Durable encoding-inclusive 512 acceptance target missed: report={sys.argv[1]} {result}",
              file=sys.stderr)

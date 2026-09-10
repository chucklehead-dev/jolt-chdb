#!/usr/bin/env python3
"""Generate the independent Decimal oracle for public Durable lease writes."""

from decimal import Decimal
import json


def main():
    now_ms = Decimal("1788230400125")
    ttl_ms = Decimal("375")
    acquisition_ms = now_ms + ttl_ms
    recovery_renewal_ms = max(acquisition_ms + Decimal("1"), now_ms + ttl_ms)
    milliseconds_per_second = Decimal("1000")
    fixture = {
        "expected_expires_at": [
            format(acquisition_ms / milliseconds_per_second, ".3f"),
            format(recovery_renewal_ms / milliseconds_per_second, ".3f"),
        ],
        "generator": {
            "arithmetic": "python decimal.Decimal",
            "script": "scripts/generate-durable-python-time-fixture.py",
        },
        "inputs_ms": {
            "heartbeat_interval": "125",
            "lease_ttl": format(ttl_ms, "f"),
            "now": format(now_ms, "f"),
        },
        "protocol": {
            "commit": "66643e5030fb73c30ac5cdd31d4c7858ea040ed0",
            "document": "docs/durable/protocol-v1.mdx",
            "repository": "https://github.com/chdb-io/chdb.git",
        },
        "schema": 1,
    }
    print(json.dumps(fixture, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()

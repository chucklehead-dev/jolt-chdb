"""Read-only AWS scope/lifecycle preflight; prints no identifiers or policies.

This accepts a narrow known policy shape, not arbitrary IAM semantics. Unknown
conditions/denies/boundaries require explicit review instead of being ignored.
No object, bucket, lifecycle, policy or role mutation is performed here.
"""
import fnmatch
import hashlib
import json
import os
import re
import subprocess
import sys
import urllib.parse


class PreflightFailure(Exception):
    pass


METADATA_OPERATIONS = frozenset({
    ("sts", "get-caller-identity"), ("iam", "get-role"),
    ("iam", "list-role-policies"), ("iam", "list-attached-role-policies"),
    ("iam", "get-role-policy"), ("iam", "get-policy"),
    ("iam", "get-policy-version"),
    ("s3api", "get-bucket-lifecycle-configuration"),
    ("s3api", "get-bucket-versioning"),
})


def require(ok, code):
    if not ok:
        raise PreflightFailure(code)


def aws(*args):
    operation = args[:2]
    require(operation in METADATA_OPERATIONS, "unsupported-metadata-operation")
    # Report only an allowlisted service/API label, never arguments or stderr.
    failure = "metadata-" + "-".join(operation) + "-failed"
    try:
        result = subprocess.run(
            ["aws", *args, "--output", "json", "--no-cli-pager",
             "--cli-connect-timeout", "10", "--cli-read-timeout", "20"],
            capture_output=True, timeout=40, check=False,
        )
    except (subprocess.TimeoutExpired, OSError):
        result = None
    # Raise outside the handler so the public error does not retain the original
    # TimeoutExpired/OSError as __context__, even under explicit inspection.
    require(result is not None, failure)
    # Never relay stderr: CLI errors may name accounts, role ARNs or buckets.
    require(result.returncode == 0, failure)
    return json.loads(result.stdout)


def document(value):
    if isinstance(value, str):
        value = json.loads(urllib.parse.unquote(value))
    require(isinstance(value, dict), "invalid-policy-document")
    return value


def items(value):
    return value if isinstance(value, list) else [value]


def policy_scope(documents, bucket, account):
    bucket_arn = f"arn:aws:s3:::{bucket}"
    object_scope = bucket_arn + "/ci/jolt-chdb/*"
    granted = set()
    for policy in documents:
        for statement in items(policy.get("Statement", [])):
            require(isinstance(statement, dict), "invalid-policy-statement")
            # A conservative narrow gate is intentionally not an IAM simulator.
            require(statement.get("Effect") == "Allow", "policy-needs-deny-review")
            require(not any(k in statement for k in ("NotAction", "NotResource", "Principal", "NotPrincipal")),
                    "policy-needs-shape-review")
            condition = statement.get("Condition", {})
            accepted_condition = {"StringEquals": {"s3:ResourceAccount": account}}
            require(condition in ({}, accepted_condition), "policy-needs-condition-review")
            actions = items(statement.get("Action", []))
            resources = items(statement.get("Resource", []))
            require(all(isinstance(x, str) for x in actions + resources), "invalid-policy-members")
            relevant = [action for action in actions if action.lower().startswith("s3:")]
            if not relevant:
                continue
            object_actions = [a for a in relevant if any(fnmatch.fnmatchcase(required.lower(), a.lower())
                              for required in ("s3:GetObject", "s3:PutObject"))]
            if object_actions:
                require(resources == [object_scope], "object-policy-scope-mismatch")
                for required in ("s3:GetObject", "s3:PutObject"):
                    if any(fnmatch.fnmatchcase(required.lower(), a.lower()) for a in object_actions):
                        granted.add(required)
            else:
                require(all(r == bucket_arn for r in resources), "bucket-policy-scope-mismatch")
    require(granted == {"s3:GetObject", "s3:PutObject"}, "missing-object-capabilities")


def lifecycle_scope(lifecycle, versioning, prefix):
    require(versioning.get("Status") in (None, "Enabled", "Suspended"),
            "invalid-versioning-status")
    rules = lifecycle.get("Rules", [])
    require(isinstance(rules, list), "invalid-lifecycle")
    applicable = []
    for rule in rules:
        if rule.get("Status") != "Enabled":
            continue
        selection = rule.get("Filter", {"Prefix": rule.get("Prefix", "")})
        # Objects are not tagged. Do not silently accept tag/size conditions.
        if not isinstance(selection, dict) or set(selection) - {"Prefix"}:
            continue
        selected_prefix = selection.get("Prefix", "")
        if isinstance(selected_prefix, str) and prefix.startswith(selected_prefix):
            applicable.append(rule)
    def bounded(value):
        return isinstance(value, int) and not isinstance(value, bool) and 1 <= value <= 30
    require(any(bounded(r.get("Expiration", {}).get("Days")) for r in applicable),
            "missing-bounded-current-expiry")
    if versioning.get("Status") in ("Enabled", "Suspended"):
        require(any(bounded(r.get("NoncurrentVersionExpiration", {}).get("NoncurrentDays"))
                    and "NewerNoncurrentVersions" not in r.get("NoncurrentVersionExpiration", {})
                    for r in applicable), "missing-bounded-noncurrent-expiry")


def main():
    role = os.environ.get("TASK_ROLE_ARN", "")
    bucket = os.environ.get("TASK_BUCKET", "")
    prefix = os.environ.get("TASK_PREFIX", "")
    match = re.fullmatch(r"arn:aws:iam::([0-9]{12}):role/(?:[A-Za-z0-9+=,.@_-]+/)*([A-Za-z0-9+=,.@_-]+)", role)
    require(match is not None and re.fullmatch(r"[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]", bucket),
            "invalid-configuration")
    require(re.fullmatch(r"ci/jolt-chdb/[0-9]+-[0-9]+/typed-window", prefix), "invalid-prefix")
    account, name = match.groups()
    identity = aws("sts", "get-caller-identity")
    require(identity.get("Account") == account and
            identity.get("Arn", "").startswith(f"arn:aws:sts::{account}:assumed-role/{name}/"),
            "assumed-role-mismatch")
    role_record = aws("iam", "get-role", "--role-name", name)["Role"]
    require(role_record.get("Arn") == role and not role_record.get("PermissionsBoundary"),
            "role-needs-boundary-review")
    inline = aws("iam", "list-role-policies", "--role-name", name).get("PolicyNames", [])
    attached = aws("iam", "list-attached-role-policies", "--role-name", name).get("AttachedPolicies", [])
    require(len(inline) + len(attached) <= 16, "policy-count-limit")
    policies = [document(aws("iam", "get-role-policy", "--role-name", name,
                            "--policy-name", p)["PolicyDocument"]) for p in inline]
    for policy in attached:
        arn = policy["PolicyArn"]
        version = aws("iam", "get-policy", "--policy-arn", arn)["Policy"]["DefaultVersionId"]
        policies.append(document(aws("iam", "get-policy-version", "--policy-arn", arn,
                                     "--version-id", version)["PolicyVersion"]["Document"]))
    policy_scope(policies, bucket, account)
    lifecycle = aws("s3api", "get-bucket-lifecycle-configuration", "--bucket", bucket,
                    "--expected-bucket-owner", account)
    versioning = aws("s3api", "get-bucket-versioning", "--bucket", bucket,
                     "--expected-bucket-owner", account)
    lifecycle_scope(lifecycle, versioning, prefix)
    digest = hashlib.sha256(json.dumps({"policies": policies, "lifecycle": lifecycle,
        "versioning": versioning}, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    print(json.dumps({"preflight": "passed", "policy_documents": len(policies),
        "configuration_sha256": digest, "max_expiry_days": 30,
        "scope": "current-typed-window", "proof": "narrow-policy-and-lifecycle-only"}))


if __name__ == "__main__":
    try:
        main()
    except PreflightFailure as error:
        print(json.dumps({"preflight": "failed", "reason": str(error)}))
        sys.exit(1)
    except Exception:
        print(json.dumps({"preflight": "failed", "reason": "metadata-or-parser-failed"}))
        sys.exit(1)

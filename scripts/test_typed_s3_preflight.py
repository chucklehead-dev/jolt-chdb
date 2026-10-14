"""Pure metadata controls. No AWS credentials, role assumption or network."""
import copy
import importlib.util
import pathlib
import subprocess
import types
from unittest.mock import patch

path = pathlib.Path(__file__).with_name("typed_s3_preflight.py")
spec = importlib.util.spec_from_file_location("preflight", path)
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)
policy = {"Statement": [{"Effect": "Allow", "Action": ["s3:GetObject", "s3:PutObject"],
                        "Resource": "arn:aws:s3:::fixture-bucket/ci/jolt-chdb/*"}]}
prefix = "ci/jolt-chdb/123-1/typed-window"
lifecycle = {"Rules": [{"Status": "Enabled", "Filter": {"Prefix": "ci/jolt-chdb/"},
                        "Expiration": {"Days": 7}, "NoncurrentVersionExpiration": {"NoncurrentDays": 7}}]}


def verdict(label, f, expected):
    try:
        f()
        actual = True
    except module.PreflightFailure:
        actual = False
    assert actual == expected, label
    print(f"{label}: {'accepted' if actual else 'rejected'}")


verdict("valid scoped grant", lambda: module.policy_scope([policy], "fixture-bucket", "123456789012"), True)
for label, field, value in [
    ("wrong object prefix", "Resource", "arn:aws:s3:::fixture-bucket/ci/oscope/*"),
    ("broad resource", "Resource", "*"),
    ("missing put", "Action", ["s3:GetObject"]),
    ("deny review", "Effect", "Deny"),
    ("unknown condition", "Condition", {"StringEquals": {"custom:tag": "yes"}}),
]:
    changed = copy.deepcopy(policy)
    changed["Statement"][0][field] = value
    verdict(label, lambda p=changed: module.policy_scope([p], "fixture-bucket", "123456789012"), False)
changed = copy.deepcopy(policy)
changed["Statement"][0]["Condition"] = {"StringEquals": {"s3:ResourceAccount": "123456789012"}}
verdict("known matching resource account", lambda: module.policy_scope([changed], "fixture-bucket", "123456789012"), True)
verdict("wrong resource account", lambda: module.policy_scope([changed], "fixture-bucket", "000000000000"), False)
verdict("bounded versioned expiry", lambda: module.lifecycle_scope(lifecycle, {"Status": "Enabled"}, prefix), True)
for label, mutate in [
    ("disabled expiry", lambda r: r.update(Status="Disabled")),
    ("wrong lifecycle prefix", lambda r: r.update(Filter={"Prefix": "ci/oscope/"})),
    ("tag-dependent expiry", lambda r: r.update(Filter={"Tag": {"Key": "x", "Value": "y"}})),
    ("unbounded age", lambda r: r.update(Expiration={"Days": 365})),
    ("noncurrent retention missing", lambda r: r.pop("NoncurrentVersionExpiration")),
    ("noncurrent versions retained", lambda r: r.update(NoncurrentVersionExpiration={"NoncurrentDays": 7, "NewerNoncurrentVersions": 1})),
]:
    changed = copy.deepcopy(lifecycle)
    mutate(changed["Rules"][0])
    verdict(label, lambda d=changed: module.lifecycle_scope(d, {"Status": "Enabled"}, prefix), False)
verdict("suspended bucket still needs old-version expiry",
        lambda: module.lifecycle_scope({"Rules": [{"Status": "Enabled", "Prefix": "", "Expiration": {"Days": 7}}]},
                                       {"Status": "Suspended"}, prefix), False)
verdict("unknown versioning status", lambda: module.lifecycle_scope(lifecycle, {"Status": "Unknown"}, prefix), False)
print("preflight pure controls: 17 passed; not an IAM simulator or live cloud proof")

sentinel = "fixture-sensitive-cli-detail"
for label, outcome in [
    ("CLI failure", types.SimpleNamespace(returncode=1, stdout=b"", stderr=sentinel.encode())),
    ("timeout", subprocess.TimeoutExpired(["aws", sentinel], 40)),
    ("launch failure", OSError(sentinel)),
]:
    kwargs = {"side_effect": outcome} if isinstance(outcome, Exception) else {"return_value": outcome}
    with patch.object(module.subprocess, "run", **kwargs) as run:
        try:
            module.aws("iam", "get-role", "--role-name", sentinel)
            raise AssertionError("failure was accepted")
        except module.PreflightFailure as error:
            assert str(error) == "metadata-iam-get-role-failed"
            assert sentinel not in str(error)
        assert run.call_count == 1
    print(f"sanitized {label}: passed")
with patch.object(module.subprocess, "run") as run:
    try:
        module.aws("s3api", "delete-object", sentinel)
        raise AssertionError("mutation was accepted")
    except module.PreflightFailure as error:
        assert str(error) == "unsupported-metadata-operation"
    assert run.call_count == 0
print("metadata operation/error controls: 4 passed; no AWS calls")

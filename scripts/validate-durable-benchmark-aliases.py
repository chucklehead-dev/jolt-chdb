#!/usr/bin/env python3
"""Allow only additive, self-contained bench/test aliases in committed deps.edn."""

import json
import re
import subprocess
import sys


class Invalid(Exception):
    pass


class Reader:
    def __init__(self, source):
        self.source = source
        self.pos = 0

    def skip(self):
        while self.pos < len(self.source):
            char = self.source[self.pos]
            if char.isspace() or char == ",":
                self.pos += 1
            elif char == ";":
                end = self.source.find("\n", self.pos)
                self.pos = len(self.source) if end < 0 else end + 1
            else:
                break

    def value(self):
        self.skip()
        if self.pos >= len(self.source):
            raise Invalid("missing value")
        char = self.source[self.pos]
        self.pos += 1
        if char in "{[":
            close = "}" if char == "{" else "]"
            items = []
            while True:
                self.skip()
                if self.pos >= len(self.source):
                    raise Invalid("unclosed collection")
                if self.source[self.pos] == close:
                    self.pos += 1
                    break
                items.append(self.value())
            if char == "[":
                return ("vector", tuple(items))
            if len(items) % 2:
                raise Invalid("odd map")
            result = {}
            for key, value in zip(items[::2], items[1::2]):
                if not isinstance(key, tuple) or key[0] != "atom" or key in result:
                    raise Invalid("unsupported or duplicate map key")
                result[key] = value
            return result
        if char == '"':
            end = self.pos
            while end < len(self.source):
                if self.source[end] == "\\":
                    end += 2
                elif self.source[end] == '"':
                    value = json.loads(self.source[self.pos - 1:end + 1])
                    self.pos = end + 1
                    return ("string", value)
                else:
                    end += 1
            raise Invalid("unclosed string")
        if char in "]}()#^'`~@\\":
            raise Invalid("unsupported EDN form")
        start = self.pos - 1
        while self.pos < len(self.source) and self.source[self.pos] not in " \t\r\n,;{}[]()\"":
            self.pos += 1
        atom = self.source[start:self.pos]
        if not re.fullmatch(r"[A-Za-z0-9_:.+!?*/=<>-]+", atom):
            raise Invalid("unsupported atom")
        return ("atom", atom)

    def read(self):
        value = self.value()
        self.skip()
        if self.pos != len(self.source):
            raise Invalid("trailing form")
        return value


def git(repo, *args):
    return subprocess.check_output(["git", "-C", repo, *args], stderr=subprocess.DEVNULL)


def atom(value):
    return ("atom", value)


def string(value):
    return ("string", value)


def validate(repo, base, head):
    statuses = git(repo, "diff", "--no-renames", "--name-status", "-z", base, head).split(b"\0")
    if statuses[-1:] != [b""] or len(statuses[:-1]) % 2:
        raise Invalid("invalid name-status diff")
    changed = {path.decode(): status.decode() for status, path in zip(statuses[::2], statuses[1::2])}
    if changed.get("deps.edn") != "M":
        raise Invalid("deps.edn must be modified")
    before = Reader(git(repo, "show", f"{base}:deps.edn").decode()).read()
    after = Reader(git(repo, "show", f"{head}:deps.edn").decode()).read()
    if not isinstance(before, dict) or not isinstance(after, dict):
        raise Invalid("deps.edn root must be a map")
    old_aliases = before.pop(atom(":aliases"), None)
    new_aliases = after.pop(atom(":aliases"), None)
    if before != after or not isinstance(old_aliases, dict) or not isinstance(new_aliases, dict):
        raise Invalid("root dependencies or options changed")
    if any(new_aliases.get(key) != value for key, value in old_aliases.items()):
        raise Invalid("existing alias changed")
    added = set(new_aliases) - set(old_aliases)
    if not added:
        raise Invalid("no new aliases")
    if set(new_aliases) != set(old_aliases) | added:
        raise Invalid("alias removed")
    for key in added:
        if key[0] != "atom" or not re.fullmatch(r":[a-z][a-z0-9-]*", key[1]):
            raise Invalid("invalid new alias name")
        alias = new_aliases[key]
        if not isinstance(alias, dict) or set(alias) != {atom(":extra-paths"), atom(":main-opts")}:
            raise Invalid("unknown alias option")
        paths = alias[atom(":extra-paths")]
        opts = alias[atom(":main-opts")]
        if (not isinstance(paths, tuple) or paths[0] != "vector" or not paths[1]
                or len(set(paths[1])) != len(paths[1])
                or any(path not in {string("bench"), string("test")} for path in paths[1])):
            raise Invalid("unsupported extra-paths")
        if not isinstance(opts, tuple) or opts[0] != "vector" or len(opts[1]) not in (2, 3):
            raise Invalid("unsupported main-opts")
        args = opts[1]
        if args[0] != string("-m") or args[1][0] != "string":
            raise Invalid("main-opts must select namespace")
        if len(args) == 3 and (args[2] != string("--native") or not key[1].endswith("-native-test")):
            raise Invalid("unsupported main argument")
        namespace = args[1][1]
        if not re.fullmatch(r"[a-z][a-z0-9-]*(?:\.[a-z][a-z0-9-]*)+", namespace):
            raise Invalid("invalid namespace")
        relative = namespace.replace(".", "/").replace("-", "_")
        if not any(changed.get(f"{path[1]}/{relative}.{ext}") == "A"
                   for path in paths[1] for ext in ("clj", "cljc")):
            raise Invalid("main namespace was not newly added")


if __name__ == "__main__":
    try:
        if len(sys.argv) != 4:
            raise Invalid("expected repository and exact base/head revisions")
        validate(*sys.argv[1:])
    except (Invalid, KeyError, TypeError, OSError, UnicodeError, ValueError,
            subprocess.CalledProcessError):
        sys.exit(1)

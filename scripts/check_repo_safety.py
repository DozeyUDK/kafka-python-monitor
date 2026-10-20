"""Fail CI when tracked files expose sensitive material or local workspace data."""
import pathlib
import re
import subprocess
import sys


FORBIDDEN = (
    ("private-key-block", re.compile(r"-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----")),
    ("github-token", re.compile(r"\b(?:gh[pousr]_[A-Za-z0-9]{20,}|github_pat_[A-Za-z0-9_]{30,})\b")),
    ("cloud-access-key", re.compile(r"\b(?:AKIA|ASIA)[A-Z0-9]{16}\b")),
    ("openai-secret", re.compile(r"\bsk-(?:proj-|live-)[A-Za-z0-9_-]{24,}\b")),
    ("local-workspace", re.compile("/home/" + "dozey/" + "ChatGPT_" + "Editable/")),
)
BLOCKED_SUFFIXES = (".pem", ".p12", ".pfx", ".jks", ".keystore", ".key")
BLOCKED_NAMES = (".env", ".env.local", ".env.production", ".env.test", "id_rsa", "id_ed25519")


def scan(paths):
    findings = []
    for name in paths:
        path = pathlib.PurePosixPath(name)
        if (path.name in BLOCKED_NAMES or name.startswith((".pytest_cache/", "kafka-monitor/"))
                or name.endswith(BLOCKED_SUFFIXES)):
            findings.append("%s: forbidden tracked path" % name)
            continue
        file = pathlib.Path(name)
        if not file.is_file():
            continue
        if file.stat().st_size > 10 * 1024 * 1024:
            findings.append("%s: oversized file requires review" % name)
            continue
        data = file.read_bytes()
        if b"\x00" in data:
            continue
        source = data.decode("utf-8", "replace")
        for label, pattern in FORBIDDEN:
            if pattern.search(source):
                findings.append("%s: %s" % (name, label))
    return findings


def main():
    result = subprocess.check_output(["git", "ls-files", "-z"])
    names = [n for n in result.decode("utf-8").split("\0") if n]
    findings = scan(names)
    for line in findings:
        print(line, file=sys.stderr)
    print("Tracked-file safety scan: %d checked, %d findings" % (len(names), len(findings)))
    return 1 if findings else 0


if __name__ == "__main__":
    raise SystemExit(main())

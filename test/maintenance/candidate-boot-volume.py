#!/usr/bin/env python3
"""One-shot closed candidate boot test; never publishes or retires its source."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import stat
import types
import uuid

HELPER_REVISION = "f8f04c8a36394b75dc4243e360c08f7ffa6e6522"
HELPER_SHA256 = "21884ae9d92572fba8220f53d0fa4558a2671bce27abcb2a39fd081d8b80c88e"


def helper(path):
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    with os.fdopen(fd, "rb") as stream:
        info = os.fstat(stream.fileno())
        data = stream.read(1 << 20)
    if (not stat.S_ISREG(info.st_mode) or info.st_uid != 1000 or info.st_mode & 0o022
            or info.st_nlink != 1 or hashlib.sha256(data).hexdigest() != HELPER_SHA256):
        raise ValueError("approved_helper_source_mismatch")
    module = types.ModuleType("approved_copy_volume")
    exec(compile(data, path, "exec"), module.__dict__)
    return module


def candidate(receipt):
    if receipt.get("status") == "complete_unpublished_chain":
        if receipt.get("published") is not False or receipt.get("first", {}).get("full_clean_complete") is not True:
            raise ValueError("cleanup_chain_incomplete")
        receipt = receipt["second"]
    tool = receipt.get("tool_report", {})
    if (receipt.get("status") != "verified_private_copy_run" or receipt.get("operation") != "rowid-rebuild"
            or receipt.get("stopped_writer_retired") is not True or receipt.get("source_unchanged") is not True
            or receipt.get("volume_remaining_container_references") != 0
            or tool.get("status") != "complete_unpublished" or tool.get("auto_vacuum") != 2
            or tool.get("source_integrity_checked") is not True or tool.get("source_read_only_fds") is not True
            or receipt.get("closed_copy_sha256") != tool.get("candidate_sha256")):
        raise ValueError("closed_typed_candidate_required")
    run, volume, nonce = receipt["run"], receipt["volume"], receipt["volume_nonce"]
    if (not re.fullmatch(r"ao-copy-[0-9a-f]{32}", run) or volume != run + "-private"
            or not re.fullmatch(r"[0-9a-f]{32}", nonce)
            or not re.fullmatch(r"[0-9a-f]{64}", receipt["closed_copy_sha256"])):
        raise ValueError("candidate_volume_identity")
    token = receipt["copy_stat"]
    if (len(token) != 7 or any(type(v) is not int or v < 0 for v in token)
            or token[2:6] != [1000, 1000, 0o600, 1] or token[6] <= 0):
        raise ValueError("candidate_private_token")
    return run, volume, nonce, token, receipt["closed_copy_sha256"]


def request(value, m):
    if set(value) != {"receipt", "receipt_sha256", "image_id", "test_binary", "test_binary_sha256", "ao_binary", "ao_binary_sha256", "source_revision", "budget"}:
        raise ValueError("boot_request_shape")
    if not re.fullmatch(r"sha256:[0-9a-f]{64}", value["image_id"]) or not re.fullmatch(r"[0-9a-f]{40}", value["source_revision"]):
        raise ValueError("boot_image_or_source")
    for role in ("test_binary", "ao_binary"):
        m.plain_path(value[role])
        info = Path(value[role]).lstat()
        if (not stat.S_ISREG(info.st_mode) or info.st_uid != 1000 or info.st_nlink != 1
                or info.st_mode & 0o022 or m.digest(Path(value[role])) != value[role + "_sha256"]):
            raise ValueError("boot_tool_identity")
    b = value["budget"]
    if (set(b) != {"cpu", "memory_bytes", "pids", "tmp_bytes", "log_bytes", "control_seconds", "wall_seconds", "outer_seconds", "nice", "io_class"}
            or any(type(v) is not int or v <= 0 for v in b.values()) or b["cpu"] != 1
            or b["memory_bytes"] > 2 << 30 or b["tmp_bytes"] > 512 << 20 or b["log_bytes"] > 1 << 20
            or b["outer_seconds"] <= b["wall_seconds"] + b["control_seconds"] or b["pids"] > 256 or b["nice"] != 19 or b["io_class"] != 3):
        raise ValueError("boot_budget")
    return candidate(m.load_manifest(value["receipt"], value["receipt_sha256"]))


def execute(value, m):
    source_run, source_volume, source_nonce, source_token, sha = request(value, m)
    b, image = value["budget"], value["image_id"]
    control, run = b["control_seconds"], "ao-copy-" + uuid.uuid4().hex
    volume, cid, nonce = run + "-private", None, None
    base = {"budget": b, "image_id": image}
    source = f"/snapshot/{source_run}/candidate.sqlite3"
    mount = "type=volume,src=" + source_volume + ",dst=/snapshot,readonly,volume-nocopy"
    common = lambda user: m.common(base, run, user)
    transient = lambda args: m.transient(args, run, b["wall_seconds"], control, b)

    def check_source():
        v = json.loads(m.docker(["volume", "inspect", source_volume], control))[0]
        if (v["Name"] != source_volume or v["Driver"] != "local" or v.get("Options") or v.get("Labels", {}).get("org.albanews.rc.project") != "albanews-quality" or v.get("Labels", {}).get(m.LABEL) != source_run
                or v["Labels"].get(m.VOLUME_NONCE) != source_nonce or m.reference_ids(source_volume, control)):
            raise ValueError("candidate_closed_volume_identity_or_references")
        text = transient([*common("1000:1000"), "--mount", mount, "--entrypoint", "/bin/sh", image, "-ec",
                          'test -f "$1"; test ! -L "$1"; for s in -wal -shm -journal; do test ! -e "$1$s"; test ! -L "$1$s"; done; '
                          'stat -c "%d %i %u %g %a %h %s" -- "$1"; sha256sum -- "$1"', "--", source])
        lines = text.splitlines()
        observed = [int(v, 8 if i == 4 else 10) for i, v in enumerate(lines[0].split())]
        if len(lines) != 2 or observed != source_token or lines[1].split()[0] != sha or m.reference_ids(source_volume, control):
            raise ValueError("candidate_stat_or_digest_changed")

    check_source()
    root = Path(json.loads(m.docker(["info", "--format", "{{json .DockerRootDir}}"], control)))
    m.plain_path(root)
    if root.stat().st_uid != 0 or os.access(root, os.X_OK):
        raise ValueError("docker_storage_not_private_from_host_uid")
    space = os.statvfs(root)
    if space.f_bavail * space.f_frsize < source_token[6] + (1 << 30):
        raise ValueError("boot_copy_disk_headroom")
    if m.docker(["volume", "ls", "-q", "--filter", "name=^" + volume + "$"], control):
        raise ValueError("new_boot_volume_exists")
    m.docker(["volume", "create", "--driver", "local", "--label", m.LABEL + "=" + run, volume], control)
    try:
        v = json.loads(m.docker(["volume", "inspect", volume], control))[0]
        nonce = v.get("Labels", {}).get(m.VOLUME_NONCE)
        if (v["Name"] != volume or v["Driver"] != "local" or v.get("Options") or v.get("Labels", {}).get(m.LABEL) != run
                or not re.fullmatch(r"[0-9a-f]{32}", nonce or "") or m.reference_ids(volume, control)):
            raise ValueError("new_boot_volume_identity")
        target = "type=volume,src=" + volume + ",dst=/work,volume-nocopy"
        transient([*common("0:0"), "--cap-add", "CHOWN", "--mount", target, "--entrypoint", "/bin/sh", image, "-ec",
                   'test -z "$(ls -A /work)"; chmod 0700 /work; chown 1000:1000 /work'])
        if m.reference_ids(volume, control):
            raise ValueError("boot_initialization_reference")
        args = [*common("1000:1000"), "--mount", mount, "--mount", target,
                "--mount", "type=bind,src=" + value["test_binary"] + ",dst=/tool/ao-candidate-boot.test,readonly",
                "--mount", "type=bind,src=" + value["ao_binary"] + ",dst=/tool/ao,readonly",
                "--env", "HOME=/tmp", "--env", "TMPDIR=/work", "--env", "GOMAXPROCS=1", "--env", "GOMEMLIMIT=1400MiB",
                "--env", "AO_RECOVERY_CANDIDATE_SOURCE=" + source, "--env", "AO_RECOVERY_CANDIDATE_SHA256=" + sha,
                "--env", "AO_RECOVERY_CANDIDATE_TOKEN=" + json.dumps(source_token),
                "--entrypoint", "/usr/bin/nice", image, "-n", str(b["nice"]), "/usr/bin/ionice", "-c", str(b["io_class"]),
                "/tool/ao-candidate-boot.test", "-test.run=^TestClosedCandidateBoot$", "-test.timeout=" + str(b["wall_seconds"]) + "s"]
        cid = m.launch(args, control)
        state = json.loads(m.docker(["inspect", cid], control))[0]
        mounts = {v["Destination"]: v for v in state["Mounts"]}
        if (set(mounts) != {"/snapshot", "/work", "/tool/ao", "/tool/ao-candidate-boot.test"}
                or {k for k, v in mounts.items() if v["RW"]} != {"/work"}
                or mounts["/snapshot"].get("Name") != source_volume or mounts["/work"].get("Name") != volume
                or any(mounts[k].get("Source") != value[r] for k, r in (("/tool/ao", "ao_binary"), ("/tool/ao-candidate-boot.test", "test_binary")))
                or state["Config"]["User"] != "1000:1000" or state["Image"] != image
                or not state["HostConfig"]["ReadonlyRootfs"] or state["HostConfig"]["NetworkMode"] != "none"
                or state["HostConfig"].get("PidMode") not in ("", "private")
                or m.reference_ids(volume, control) != {cid} or m.reference_ids(source_volume, control) != {cid}):
            raise ValueError("boot_container_isolation_or_reference")
        m.docker(["exec", cid, "/bin/sh", "-ec", 'umask 077; test ! -e /work/boot-approved; : > /work/boot-approved'], control)
        if int(m.docker(["wait", cid], b["outer_seconds"])) != 0:
            raise ValueError("private_boot_test_failed")
        m.retire_container(cid, run, control, remove=True)
        cid = None
        if m.reference_ids(volume, control):
            raise ValueError("closed_boot_reference_remains")
        text = transient([*common("1000:1000"), "--mount", target + ",readonly", "--entrypoint", "/bin/cat", image, "/work/boot-result-safe.json"])
        result = json.loads(text)
        if result.get("status") != "private_candidate_boot_passed" or result.get("source_sha256") != sha:
            raise ValueError("boot_result_mismatch")
        check_source()
        return {"status": "verified_private_candidate_boot", "run": run, "volume": volume, "volume_nonce": nonce,
                "source_run": source_run, "source_volume": source_volume, "source_unchanged": True,
                "source_sha256": sha, "result": result, "published": False, "production_actions": []}
    except (OSError, ValueError, KeyError, TypeError, IndexError) as error:
        reason = str(error) if re.fullmatch(r"[a-z_]+", str(error)) else "boot_metadata_or_io"
        failure = m.CopyFailure("candidate_boot", reason)
        failure.retained_objects = [{"run": run, "volume": volume, "volume_nonce": nonce}]
        raise failure from None
    finally:
        m.retire_run_containers(run, control, remove=True)
        # The private target remains for inspection; SOURCE IS NEVER RETIRED.


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--helper", required=True)
    p.add_argument("--request", required=True)
    p.add_argument("--request-sha256", required=True)
    p.add_argument("--apply", action="store_true")
    args = p.parse_args()
    try:
        m = helper(args.helper)
        value = m.load_manifest(args.request, args.request_sha256)
        request(value, m)
        print(json.dumps(execute(value, m) if args.apply else {"status": "plan_only", "helper_source": HELPER_REVISION}))
        return 0
    except (OSError, ValueError, KeyError, TypeError, IndexError) as error:
        reason = str(error) if re.fullmatch(r"[a-z_]+", str(error)) else "boot_metadata_or_io"
        print(json.dumps({"status": "failed_retained_private_target", "reason": reason, "production_actions": [], "retained_objects": getattr(error, "retained_objects", [])}))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

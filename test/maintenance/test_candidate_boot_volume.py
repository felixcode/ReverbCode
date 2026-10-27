"""Synthetic contract checks; no Docker, native host, or real database calls."""
import copy
import importlib.util
import json
import os
from pathlib import Path
import tempfile
import types
import unittest
from unittest.mock import patch

spec = importlib.util.spec_from_file_location("boot", Path(__file__).with_name("candidate-boot-volume.py"))
boot = importlib.util.module_from_spec(spec)
spec.loader.exec_module(boot)


STAGES = ["raw", "outputs", "message", "activity", "turn", "session", "builtin:change_log", "builtin:telemetry_event", "builtin:notifications", "builtin:agent_switch_failure_outbox", "builtin:agent_switch_failure_receipts"]


def receipt(operation="rowid-rebuild"):
    # Synthetic values, with the actual managed f8 closed-receipt/tool shapes.
    clean = operation == "history-clean"
    run = "ao-copy-" + ("a" if clean else "b") * 32
    source, closed = ("1" * 64, "2" * 64) if clean else ("2" * 64, "3" * 64)
    tool = {"source_sha256": source}
    if clean:
        tool.update(protocol=1, status="complete", complete=True, clean_requested=True,
                    as_of="2026-10-25T21:28:16.835298Z", cutoff="2026-10-23T22:28:16.835298Z",
                    raw_audit_fold=dict(status="complete", apply=True, complete=True, source_sha256=source),
                    stages=[dict(stage=name, complete=True, coverage_verified=True) for name in STAGES],
                    identity_before=[], identity_after=[], pages_before={}, pages_after={},
                    allocation_before={"available": True}, allocation_after={"available": True}, rebuild_performed=False)
    else:
        tool.update(status="complete_unpublished", apply=True, auto_vacuum=2,
                    source_integrity_checked=True, source_read_only_fds=True, candidate_sha256=closed,
                    source_bytes=4096, candidate_bytes=4096, schema_objects=200, tables=[])
    value = dict(status="verified_private_copy_run", operation=operation, run=run, volume=run+"-private",
                 volume_nonce="c"*32, copy_stat=[1, 3 if clean else 4, 1000, 1000, 0o600, 1, 4096],
                 source_stat=[1, 2 if clean else 3, 1000, 1000, 0o600, 1, 4096],
                 stopped_writer_retired=True, container_exit_code=0, source_sha256=source, source_unchanged=True,
                 source_readonly_mount=True, output_only_private_volume=True, volume_remaining_container_references=0,
                 closed_copy_sha256=closed, tool_report=tool)
    if clean:
        value.update(full_clean_complete=True, full_clean_stages=11, full_clean_unknowns=[])
    return value


def chain():
    return dict(status="complete_unpublished_chain", published=False,
                first=receipt("history-clean"), second=receipt(), deferred_identity_checks=[], resolved_identity_checks=[])


def budget():
    return dict(cpu=1, memory_bytes=2 << 30, pids=128, tmp_bytes=128 << 20, log_bytes=65536,
                control_seconds=60, wall_seconds=600, outer_seconds=900, nice=19, io_class=3)


class CandidateContracts(unittest.TestCase):
    def test_complete_closed_receipt_is_accepted(self):
        r = receipt()
        got = boot.candidate(r)
        self.assertEqual(got[0], r["run"])
        self.assertEqual(got[3], r["copy_stat"])
        self.assertEqual(got[4], r["closed_copy_sha256"])

    def test_chain_requires_full_clean_and_unpublished(self):
        r = chain()
        self.assertEqual(boot.candidate(r), boot.candidate(r["second"]))
        for field in ("published", "full_clean_complete"):
            with self.subTest(field=field):
                bad = copy.deepcopy(r)
                if field == "published":
                    bad[field] = True
                else:
                    bad["first"][field] = False
                with self.assertRaisesRegex(ValueError, "cleanup_chain_incomplete"):
                    boot.candidate(bad)

    def test_partial_unknown_identity_and_side_proofs_are_rejected(self):
        changes = [("status", "partial"), ("operation", "history-clean"), ("container_exit_code", 1), ("container_exit_code", False), ("source_sha256", "e" * 64),
                   ("stopped_writer_retired", False), ("source_unchanged", False),
                   ("volume_remaining_container_references", 1), ("closed_copy_sha256", "d" * 64),
                   ("volume", "foreign"), ("volume_nonce", "invalid")]
        for field, value in changes:
            with self.subTest(field=field):
                r = receipt()
                r[field] = value
                with self.assertRaises(ValueError):
                    boot.candidate(r)
        for field, value in [("status", "partial"), ("auto_vacuum", 0), ("source_read_only_fds", False), ("source_integrity_checked", False)]:
            with self.subTest(tool_field=field):
                r = receipt()
                r["tool_report"][field] = value
                with self.assertRaises(ValueError):
                    boot.candidate(r)
        for index, value in [(2, 0), (3, 0), (4, 0o644), (5, 2), (6, 0), (0, True)]:
            with self.subTest(token_index=index):
                r = receipt()
                r["copy_stat"][index] = value
                with self.assertRaises(ValueError):
                    boot.candidate(r)

    def test_required_digest_types_and_presence_for_all_roles(self):
        missing = object()
        cases = (missing, None, "", "short", "g" * 64, 7, False, [], {})
        for role in ("standalone", "first", "second"):
            fields = (("source_sha256",), ("closed_copy_sha256",), ("tool_report", "source_sha256"))
            fields += (("tool_report", "raw_audit_fold", "source_sha256"),) if role == "first" else (("tool_report", "candidate_sha256"),)
            for path in fields:
                for bad in cases:
                    with self.subTest(role=role, field=path, value=repr(bad)):
                        value = receipt() if role == "standalone" else chain()
                        obj = value if role == "standalone" else value[role]
                        for key in path[:-1]:
                            obj = obj[key]
                        if bad is missing:
                            del obj[path[-1]]
                        else:
                            obj[path[-1]] = bad
                        with self.assertRaises(ValueError):
                            boot.candidate(value)
        for bad in (missing, None, ""):
            for role in ("standalone", "first", "second"):
                with self.subTest(paired_missing=role, value=repr(bad)):
                    value = receipt() if role == "standalone" else chain()
                    obj = value if role == "standalone" else value[role]
                    for target in (obj, obj["tool_report"]):
                        if bad is missing:
                            target.pop("source_sha256")
                        else:
                            target["source_sha256"] = bad
                    with self.assertRaises(ValueError):
                        boot.candidate(value)
        value = chain()
        value["first"] = {"full_clean_complete": True}
        value["second"].pop("source_sha256")
        value["second"]["tool_report"].pop("source_sha256")
        with self.assertRaises(ValueError):
            boot.candidate(value)

    def test_first_and_second_closed_success_are_independent(self):
        missing = object()
        fields = ["status", "operation", "stopped_writer_retired", "source_unchanged", "source_readonly_mount", "output_only_private_volume", "tool_report"]
        for role in ("first", "second"):
            for field in fields:
                for bad in (missing, None, False, "partial"):
                    with self.subTest(role=role, field=field, value=repr(bad)):
                        value = chain()
                        if bad is missing:
                            value[role].pop(field)
                        else:
                            value[role][field] = bad
                        with self.assertRaises(ValueError):
                            boot.candidate(value)
            for field in ("container_exit_code", "volume_remaining_container_references"):
                for bad in (missing, None, False, True, 1, -1, 0.0, "0"):
                    with self.subTest(role=role, counter=field, value=repr(bad)):
                        value = chain()
                        if bad is missing:
                            value[role].pop(field)
                        else:
                            value[role][field] = bad
                        with self.assertRaises(ValueError):
                            boot.candidate(value)
        for role, bad in (("first", {"full_clean_complete": True}), ("first", None), ("second", None)):
            value = chain()
            value[role] = bad
            with self.assertRaises(ValueError):
                boot.candidate(value)

    def test_complete_clean_typed_stages_and_fold_are_required(self):
        missing = object()
        for path in [("full_clean_complete",), ("full_clean_stages",), ("tool_report", "status"), ("tool_report", "complete"), ("tool_report", "clean_requested"), ("tool_report", "stages"), ("tool_report", "raw_audit_fold")]:
            for bad in (missing, None, False, "partial"):
                with self.subTest(field=path, value=repr(bad)):
                    value = chain()
                    obj = value["first"]
                    for key in path[:-1]:
                        obj = obj[key]
                    if bad is missing:
                        obj.pop(path[-1])
                    else:
                        obj[path[-1]] = bad
                    with self.assertRaises(ValueError):
                        boot.candidate(value)
        for kind in ("short", "duplicate", "partial", "coverage", "fold_complete", "fold_apply", "fold_status"):
            with self.subTest(kind=kind):
                value = chain()
                tool = value["first"]["tool_report"]
                if kind == "short":
                    tool["stages"].pop()
                elif kind == "duplicate":
                    tool["stages"][-1] = copy.deepcopy(tool["stages"][0])
                elif kind in ("partial", "coverage"):
                    tool["stages"][0]["complete" if kind == "partial" else "coverage_verified"] = False
                else:
                    tool["raw_audit_fold"][kind.removeprefix("fold_")] = False
                with self.assertRaises(ValueError):
                    boot.candidate(value)

    def test_valid_but_wrong_source_links_and_candidate_digests_are_rejected(self):
        for kind in ("first_closed", "second_sources", "first_tool", "first_fold", "candidate", "same_volume"):
            with self.subTest(kind=kind):
                value = chain()
                if kind == "first_closed":
                    value["first"]["closed_copy_sha256"] = "e" * 64
                elif kind == "second_sources":
                    value["second"]["source_sha256"] = value["second"]["tool_report"]["source_sha256"] = "e" * 64
                elif kind == "first_tool":
                    value["first"]["tool_report"]["source_sha256"] = "e" * 64
                elif kind == "first_fold":
                    value["first"]["tool_report"]["raw_audit_fold"]["source_sha256"] = "e" * 64
                elif kind == "candidate":
                    value["second"]["tool_report"]["candidate_sha256"] = "e" * 64
                else:
                    value["second"].update(run=value["first"]["run"], volume=value["first"]["volume"])
                with self.assertRaises(ValueError):
                    boot.candidate(value)

    def test_chain_unknowns_require_existing_managed_resolution(self):
        for unknown in (None, [{"table": "ordinary_view", "reason": "virtual_or_shadow_table"}], [{"table": "sqlite_sequence", "reason": "no_declared_stable_key"}]):
            value = chain()
            value["first"]["full_clean_unknowns"] = unknown
            with self.subTest(unknown=unknown), self.assertRaises(ValueError):
                boot.candidate(value)
        for field in ("deferred_identity_checks", "resolved_identity_checks"):
            for bad in (None, [None]):
                value = chain()
                value[field] = bad
                with self.subTest(field=field, value=bad), self.assertRaises(ValueError):
                    boot.candidate(value)

    def test_existing_sequence_resolution_keeps_distinct_digest_domains(self):
        value = chain()
        value["first"]["full_clean_unknowns"] = [{"table": "sqlite_sequence", "reason": "no_declared_stable_key"}]
        pending = dict(table="sqlite_sequence", rows=1, original_unknown_reason="no_declared_stable_key",
                       status="deferred_to_rebuild_typed_rowid_sequence_verification", historycopy_json_mapping_sha256="d"*64)
        managed = types.SimpleNamespace(deferred_sequence=lambda _: [pending])
        value["deferred_identity_checks"] = [pending]
        value["resolved_identity_checks"] = [pending | {"status": "verified_by_rebuild_typed_values_rowid_and_sequence", "rebuild_big_endian_rowid_sha256": "e"*64}]
        value["second"]["tool_report"]["tables"] = [dict(name="sqlite_sequence", rows=1, without_rowid=False, rowid_sha256="e"*64)]
        self.assertEqual(boot.candidate(value, managed), boot.candidate(value["second"]))
        for kind in ("wrong_hash", "wrong_rows", "bool_rows", "missing_table", "missing_resolution", "bool_pending_rows", "bool_resolved_rows"):
            bad = copy.deepcopy(value)
            if kind == "wrong_hash":
                bad["resolved_identity_checks"][0]["rebuild_big_endian_rowid_sha256"] = "d"*64
            elif kind in ("wrong_rows", "bool_rows"):
                bad["second"]["tool_report"]["tables"][0]["rows"] = 2 if kind == "wrong_rows" else True
            elif kind == "missing_table":
                bad["second"]["tool_report"]["tables"] = []
            elif kind == "bool_pending_rows":
                bad["deferred_identity_checks"][0]["rows"] = True
            elif kind == "bool_resolved_rows":
                bad["resolved_identity_checks"][0]["rows"] = True
            else:
                bad["resolved_identity_checks"] = []
            with self.subTest(kind=kind), self.assertRaises(ValueError):
                boot.candidate(bad, managed)

    def make_request(self, directory):
        tool = Path(directory) / "fixed-tool"
        tool.write_bytes(b"synthetic-tool")
        tool.chmod(0o700)
        return dict(receipt=str(Path(directory) / "receipt.json"), receipt_sha256="e" * 64,
                    image_id="sha256:" + "f" * 64, test_binary=str(tool), test_binary_sha256="1" * 64,
                    ao_binary=str(tool), ao_binary_sha256="1" * 64, source_revision="2" * 40, budget=budget())

    def test_request_limits_and_no_arbitrary_execution_surface(self):
        m = types.SimpleNamespace(plain_path=lambda _: None, digest=lambda _: "1" * 64,
                                  load_manifest=lambda *_: receipt())
        with tempfile.TemporaryDirectory() as directory:
            value = self.make_request(directory)
            boot.request(value, m)
            for key, setting in [("cpu", 2), ("memory_bytes", 3 << 30), ("pids", 512), ("nice", 1), ("io_class", 1), ("outer_seconds", 1)]:
                with self.subTest(setting=key):
                    bad = copy.deepcopy(value)
                    bad["budget"][key] = setting
                    with self.assertRaises(ValueError):
                        boot.request(bad, m)
            for key in ("command", "env", "source", "sql", "host_socket"):
                with self.subTest(extra_key=key):
                    bad = copy.deepcopy(value)
                    bad[key] = "must-not-execute"
                    with self.assertRaisesRegex(ValueError, "boot_request_shape"):
                        boot.request(bad, m)

    def test_all_stopped_or_running_source_references_refuse_before_new_volume(self):
        calls = []
        r = receipt()
        m = types.SimpleNamespace(LABEL="owner", VOLUME_NONCE="nonce", common=lambda *_: [],
                                  reference_ids=lambda *_: {"stopped-foreign-container"})
        def docker(args, _):
            calls.append(args)
            self.assertEqual(args[:2], ["volume", "inspect"])
            return json.dumps([dict(Name=r["volume"], Driver="local", Options=None,
                                   Labels={"owner": r["run"], "nonce": r["volume_nonce"], "org.albanews.rc.project": "albanews-quality"})])
        m.docker = docker
        with patch.object(boot, "request", return_value=boot.candidate(r)):
            with self.assertRaisesRegex(ValueError, "candidate_closed_volume_identity_or_references"):
                boot.execute({"budget": budget(), "image_id": "sha256:" + "f" * 64}, m)
        self.assertEqual(len(calls), 1)



if __name__ == "__main__":
    unittest.main()

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


def receipt():
    return {"status": "verified_private_copy_run", "operation": "rowid-rebuild",
            "run": "ao-copy-" + "a" * 32, "volume": "ao-copy-" + "a" * 32 + "-private",
            "volume_nonce": "b" * 32, "copy_stat": [1, 2, 1000, 1000, 0o600, 1, 4096],
            "stopped_writer_retired": True, "container_exit_code": 0, "source_sha256": "d" * 64, "source_unchanged": True,
            "volume_remaining_container_references": 0, "closed_copy_sha256": "c" * 64,
            "tool_report": {"status": "complete_unpublished", "auto_vacuum": 2,
                            "source_integrity_checked": True, "source_read_only_fds": True,
                            "candidate_sha256": "c" * 64, "source_sha256": "d" * 64}}


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
        r = dict(status="complete_unpublished_chain", published=False,
                 first={"full_clean_complete": True, "closed_copy_sha256": "d" * 64}, second=receipt())
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

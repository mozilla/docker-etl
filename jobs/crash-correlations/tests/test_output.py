"""Tests for the output writing.

The filenames, the gzip and the byte-for-byte determinism are the contract with the
Crash Stats frontend, so they're pinned here rather than left to be discovered by a
broken tab.
"""

import gzip
import json

from crash_correlations import output


class TestSignatureFilename:
    def test_is_sha1_of_the_signature(self):
        # correlation.js hashes the signature client side, so this has to match
        # exactly. Value checked against the live bucket.
        assert (
            output.signature_filename("OOM | small")
            == "19cbd625af52128aba69f233608bbc3c22e3214f.json.gz"
        )

    def test_handles_non_ascii(self):
        # Signatures contain arbitrary symbol names; hashing is over UTF-8 bytes.
        name = output.signature_filename("libc.so.6 | 火狐")
        assert name.endswith(".json.gz")
        assert len(name) == 40 + len(".json.gz")


class TestEncode:
    def test_round_trips(self):
        payload = {"total": 5, "results": [{"item": {"a": True}}]}
        assert json.loads(gzip.decompress(output.encode(payload))) == payload

    def test_is_gzip(self):
        assert output.encode({})[:2] == b"\x1f\x8b"

    def test_is_deterministic(self):
        # mtime is zeroed, so two runs on the same input give identical bytes. Without
        # this a byte diff between runs is meaningless.
        payload = {"total": 1, "results": []}
        assert output.encode(payload) == output.encode(payload)

    def test_preserves_float_counts(self):
        # The frontend divides by these; the live output has floats.
        payload = {"total": 5, "results": [{"count_reference": 100.0}]}
        decoded = json.loads(gzip.decompress(output.encode(payload)))
        assert isinstance(decoded["results"][0]["count_reference"], float)


class TestAddonRelated:
    def test_picks_over_represented_addons(self):
        results = {
            "sigA": [
                {
                    "item": {'Addon "uBlock Origin"': True},
                    "count_reference": 100.0,
                    "count_group": 50.0,
                }
            ]
        }
        # 50/100 in the signature vs 100/1000 overall, so over-represented.
        entries = output.addon_related(results, {"sigA": 100}, 1000)
        assert len(entries) == 1
        assert entries[0]["signature"] == "sigA"
        assert entries[0]["total"] == 100

    def test_ignores_under_represented_addons(self):
        results = {
            "sigA": [
                {
                    "item": {'Addon "uBlock Origin"': True},
                    "count_reference": 500.0,
                    "count_group": 5.0,
                }
            ]
        }
        assert output.addon_related(results, {"sigA": 100}, 1000) == []

    def test_ignores_non_addon_items(self):
        results = {
            "sigA": [
                {
                    "item": {"platform": "Linux"},
                    "count_reference": 100.0,
                    "count_group": 50.0,
                }
            ]
        }
        assert output.addon_related(results, {"sigA": 100}, 1000) == []

    def test_ignores_multi_item_results(self):
        # Upstream only considers single-item results here.
        results = {
            "sigA": [
                {
                    "item": {'Addon "X"': True, "platform": "Linux"},
                    "count_reference": 100.0,
                    "count_group": 50.0,
                }
            ]
        }
        assert output.addon_related(results, {"sigA": 100}, 1000) == []

    def test_skips_signatures_with_no_total(self):
        results = {"sigA": [{"item": {'Addon "X"': True}, "count_reference": 1.0,
                             "count_group": 1.0}]}
        assert output.addon_related(results, {}, 1000) == []


class TestWriter:
    def test_writes_the_expected_layout(self, tmp_path):
        writer = output.Writer(tmp_path / "out")
        writer.add("all.json.gz", {"date": "2026-08-14", "release": 100})
        writer.add_signature("release", "OOM | small", 50, [])
        count = writer.write_local()

        assert count == 2
        root = tmp_path / "out"
        assert (root / "all.json.gz").exists()
        assert (
            root / "release" / output.signature_filename("OOM | small")
        ).exists()

    def test_written_files_are_the_stored_bytes(self, tmp_path):
        writer = output.Writer(tmp_path / "out")
        payload = {"total": 5, "results": []}
        writer.add_signature("release", "sig", 5, [])
        writer.write_local()
        path = tmp_path / "out" / "release" / output.signature_filename("sig")
        assert json.loads(gzip.decompress(path.read_bytes())) == payload

    def test_clears_stale_files(self, tmp_path):
        root = tmp_path / "out"
        (root / "release").mkdir(parents=True)
        stale = root / "release" / "stale.json.gz"
        stale.write_bytes(b"old")

        writer = output.Writer(root)
        writer.add("all.json.gz", {})
        writer.write_local()

        # A signature that dropped out of the top 200 must not linger, or the frontend
        # serves data for a signature this run didn't analyse.
        assert not stale.exists()

    def test_total_bytes(self, tmp_path):
        writer = output.Writer(tmp_path / "out")
        writer.add("all.json.gz", {"date": "2026-08-14"})
        assert writer.total_bytes() > 0

    def test_upload_without_a_bucket_raises(self, tmp_path):
        writer = output.Writer(tmp_path / "out")
        try:
            writer.upload()
        except ValueError:
            return
        raise AssertionError("expected ValueError")

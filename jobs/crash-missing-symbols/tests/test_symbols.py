import datetime

import requests

from crash_missing_symbols import symbols
from crash_missing_symbols.main import Module


class TestSymbolUrl:
    def test_pdb_becomes_sym(self):
        assert symbols.symbol_url("aswJsFlt.pdb", "0E649B5B") == (
            "https://symbols.mozilla.org/aswJsFlt.pdb/0E649B5B/aswJsFlt.sym"
        )

    def test_non_pdb_keeps_name(self):
        assert symbols.symbol_url("libdyld.dylib", "49ABA86D") == (
            "https://symbols.mozilla.org/libdyld.dylib/49ABA86D/libdyld.dylib"
        )

    def test_debug_file_is_first_path_segment(self):
        url = symbols.symbol_url("firefox.pdb", "638E63D1")
        assert url.startswith("https://symbols.mozilla.org/firefox.pdb/638E63D1/")


class FakeResponse:
    ok = True


class RecordingSession:
    """Captures the URL a lookup asked for."""

    def __init__(self):
        self.urls = []

    def head(self, url, timeout=None):
        self.urls.append(url)
        return FakeResponse()


class TestAreSymbolsAvailable:
    def test_default_reproduces_the_transposed_lookup(self):
        """Parity default: the Spark job asked for a path that never exists.

        It called are_symbols_available(debug_id, debug_file) into parameters
        declared (debug_file, debug_id), so the request was
        <debug_id>/<debug_file>/<debug_id>.
        """
        session = RecordingSession()
        symbols.are_symbols_available(session, "firefox.pdb", "638E63D1")
        assert session.urls == [
            "https://symbols.mozilla.org/638E63D1/firefox.pdb/638E63D1"
        ]

    def test_fix_arg_order_asks_for_the_real_url(self):
        session = RecordingSession()
        symbols.are_symbols_available(
            session, "firefox.pdb", "638E63D1", fix_arg_order=True
        )
        assert session.urls == [
            "https://symbols.mozilla.org/firefox.pdb/638E63D1/firefox.sym"
        ]

    def test_no_request_when_either_field_is_missing(self):
        session = RecordingSession()
        for debug_file, debug_id in (("", "ABC"), ("a.pdb", ""), (None, None)):
            assert not symbols.are_symbols_available(
                session, debug_file, debug_id, fix_arg_order=True
            )
        assert session.urls == []

    def test_request_failure_is_not_fatal(self):
        class FailingSession:
            def head(self, url, timeout=None):
                raise requests.RequestException("boom")

        assert not symbols.are_symbols_available(
            FailingSession(), "a.pdb", "ABC", fix_arg_order=True
        )


class TestIsOldFirefoxModule:
    firefox_modules = {"xul.dll", "firefox.exe"}
    old_versions = {"115", "116"}

    def _module(self, name, version):
        return Module(name, version, "id", "file", 100)

    def test_old_firefox_module(self):
        assert symbols.is_old_firefox_module(
            self._module("xul.dll", "115.0.1"),
            self.firefox_modules,
            self.old_versions,
        )

    def test_current_firefox_module(self):
        assert not symbols.is_old_firefox_module(
            self._module("xul.dll", "130.0.1"),
            self.firefox_modules,
            self.old_versions,
        )

    def test_not_a_firefox_module(self):
        assert not symbols.is_old_firefox_module(
            self._module("aswJsFlt.dll", "115.0.1"),
            self.firefox_modules,
            self.old_versions,
        )

    def test_no_version_information(self):
        for version in (None, ""):
            assert not symbols.is_old_firefox_module(
                self._module("xul.dll", version),
                self.firefox_modules,
                self.old_versions,
            )

    def test_version_prefix_is_not_a_substring_match(self):
        """1150.x must not match major version 115."""
        assert not symbols.is_old_firefox_module(
            self._module("xul.dll", "1150.0"),
            self.firefox_modules,
            self.old_versions,
        )

    def test_name_match_is_case_insensitive(self):
        assert symbols.is_old_firefox_module(
            self._module("XUL.DLL", "115.0"),
            self.firefox_modules,
            self.old_versions,
        )


class TestFetchOldFirefoxVersions:
    def test_splits_on_expiry_window(self, monkeypatch):
        today = datetime.datetime(2026, 8, 7)
        history = {
            "115.0": "2023-07-04",  # > 730 days old
            "130.0": "2026-07-01",  # recent
        }

        class FakeResponse:
            def raise_for_status(self):
                pass

            def json(self):
                return history

        class FakeSession:
            def get(self, url, timeout=None):
                return FakeResponse()

        assert symbols.fetch_old_firefox_versions(FakeSession(), today) == {"115"}

import datetime

from crash_missing_symbols import report
from crash_missing_symbols.main import Module

FIREFOX = {"xul.dll"}
WINDOWS = {"kernelbase.dll"}


def build(modules):
    return report.build_body(modules, FIREFOX, WINDOWS, 3, 70)


class TestSubject:
    def test_includes_date(self):
        assert report.subject(datetime.date(2026, 8, 7)).endswith("2026-08-07")


class TestBuildBody:
    def test_firefox_module_with_debug_id_is_red(self):
        body = build([Module("xul.dll", "130.0", "ABC", "xul.pdb", 100)])
        assert '<span style="color:red;">xul.dll</span>' in body

    def test_firefox_module_without_debug_id_is_orange(self):
        body = build([Module("xul.dll", "130.0", "", "xul.pdb", 100)])
        assert '<span style="color:orange;">xul.dll</span>' in body

    def test_os_module_is_blue(self):
        body = build([Module("kernelbase.dll", "10.0", "ABC", "k.pdb", 100)])
        assert '<span style="color:blue;">kernelbase.dll</span>' in body

    def test_unknown_module_is_uncolored(self):
        body = build([Module("aswJsFlt.dll", "18.0", "ABC", "a.pdb", 100)])
        assert "aswJsFlt.dll" in body
        assert "color:" not in body

    def test_available_symbols_get_asterisk_and_note(self):
        body = build(
            [Module("a.dll", "1.0", "ABC", "a.pdb", 100, symbols_available=True)]
        )
        assert "(*)" in body
        assert "We now have symbols" in body

    def test_note_omitted_when_nothing_available(self):
        body = build([Module("a.dll", "1.0", "ABC", "a.pdb", 100)])
        assert "(*)" not in body
        assert "We now have symbols" not in body

    def test_null_version_renders_empty_not_none(self):
        body = build([Module("a.dll", None, "ABC", "a.pdb", 100)])
        assert "None" not in body

    def test_module_name_is_html_escaped(self):
        body = build([Module("<script>.dll", "1.0", "ABC", "a.pdb", 100)])
        assert "<script>" not in body
        assert "&lt;script&gt;.dll" in body

    def test_footer_reflects_actual_threshold(self):
        """The old job's footer said 2,000 while the code filtered at 70."""
        body = build([Module("a.dll", "1.0", "ABC", "a.pdb", 100)])
        assert "more than 70 crash reports" in body
        assert "past 3 days" in body

import click
import pytest

from crash_missing_symbols.main import (
    DEFAULT_EMAIL_RECIPIENTS,
    parse_recipients,
)


class TestParseRecipients:
    def test_single_address(self):
        assert parse_recipients(("a@mozilla.com",)) == ["a@mozilla.com"]

    def test_repeated_flags(self):
        assert parse_recipients(("a@mozilla.com", "b@mozilla.com")) == [
            "a@mozilla.com",
            "b@mozilla.com",
        ]

    def test_comma_separated(self):
        assert parse_recipients(("a@mozilla.com,b@mozilla.com",)) == [
            "a@mozilla.com",
            "b@mozilla.com",
        ]

    def test_whitespace_is_stripped(self):
        assert parse_recipients((" a@mozilla.com , b@mozilla.com ",)) == [
            "a@mozilla.com",
            "b@mozilla.com",
        ]

    def test_duplicates_dropped_preserving_order(self):
        assert parse_recipients(("b@mozilla.com,a@mozilla.com,b@mozilla.com",)) == [
            "b@mozilla.com",
            "a@mozilla.com",
        ]

    def test_empty_entries_ignored(self):
        assert parse_recipients(("a@mozilla.com,,",)) == ["a@mozilla.com"]

    def test_empty_input_is_an_error(self):
        for values in ((), ("",), (",",), ("  ",)):
            with pytest.raises(click.UsageError):
                parse_recipients(values)

    def test_defaults_round_trip(self):
        """The defaults must survive parsing unchanged, whatever they are."""
        assert parse_recipients(DEFAULT_EMAIL_RECIPIENTS) == list(
            DEFAULT_EMAIL_RECIPIENTS
        )

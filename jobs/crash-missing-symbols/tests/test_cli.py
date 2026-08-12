"""End to end CLI tests with BigQuery, the network and SES stubbed out."""

import contextlib
import datetime
from unittest import mock

import pytest
from click.testing import CliRunner

from crash_missing_symbols import main as cli

MODULES = [
    # Firefox module on an expired version; the old-version filter drops it.
    cli.Module("xul.dll", "115.0", "ABC123", "xul.pdb", 900),
    cli.Module("xul.dll", "130.0", "DEF456", "xul.pdb", 800),
    cli.Module("kernel32.dll", "10.0", "GHI789", "kernel32.pdb", 700),
    cli.Module("aswJsFlt.dll", "18.0", "JKL012", "aswJsFlt.pdb", 600),
    # In known_modules, so suppressed entirely.
    cli.Module("360NetBase.dll", "1.0", "MNO345", "x.pdb", 500),
]


@pytest.fixture
def run():
    """Invoke the CLI, returning (result, send_email mock)."""

    def _run(*args):
        fresh = [cli.Module(**vars(module)) for module in MODULES]
        patches = [
            mock.patch.object(cli, "query_modules", return_value=fresh),
            mock.patch.object(
                cli,
                "fetch_module_lists",
                return_value=({"360netbase.dll"}, {"xul.dll"}, {"kernel32.dll"}),
            ),
            mock.patch.object(
                cli.symbols, "fetch_old_firefox_versions", return_value={"115"}
            ),
            mock.patch.object(
                cli.symbols, "are_symbols_available", return_value=False
            ),
            mock.patch.object(cli, "send_email"),
        ]
        with contextlib.ExitStack() as stack:
            send = [stack.enter_context(patch) for patch in patches][-1]
            result = CliRunner().invoke(
                cli.main, ["--date", "2026-08-07", *args], catch_exceptions=False
            )
        return result, send

    return _run


class TestDryRun:
    def test_does_not_send(self, run):
        result, send = run("--dry-run")
        assert result.exit_code == 0
        assert not send.called

    def test_email_goes_to_stdout(self, run):
        result, _ = run("--dry-run")
        assert result.stdout.startswith("Weekly report of modules with missing")
        assert "<table" in result.stdout

    def test_recipients_reported_on_stderr_only(self, run):
        """stdout stays just the email so it can be piped to a file."""
        result, _ = run("--dry-run", "--recipient", "me@mozilla.com")
        assert "Would have mailed me@mozilla.com" in result.stderr
        assert "me@mozilla.com" not in result.stdout


class TestRecipientOptions:
    def test_defaults(self, run):
        _, send = run()
        assert send.call_args.args[3] == list(cli.DEFAULT_EMAIL_RECIPIENTS)
        assert send.call_args.args[2] == cli.DEFAULT_EMAIL_SENDER

    def test_override_replaces_defaults(self, run):
        _, send = run("--recipient", "a@mozilla.com", "--recipient", "b@mozilla.com")
        assert send.call_args.args[3] == ["a@mozilla.com", "b@mozilla.com"]

    def test_repeated_flags_deduplicated(self, run):
        _, send = run("--recipient", "a@mozilla.com", "--recipient", "a@mozilla.com")
        assert send.call_args.args[3] == ["a@mozilla.com"]

    def test_custom_sender(self, run):
        _, send = run("--sender", "alerts@mozilla.com")
        assert send.call_args.args[2] == "alerts@mozilla.com"

    def test_blank_recipient_rejected(self, run):
        result, send = run("--recipient", " ")
        assert result.exit_code == 2
        assert not send.called


class TestRunOnDays:
    """The report is built every day; only the send is gated."""

    # 2026-08-07 is a Friday, so day 5.
    def test_no_send_on_other_days(self, run):
        result, send = run("--run-on-days", "0")
        assert result.exit_code == 0
        assert not send.called

    def test_report_still_built_on_other_days(self, run):
        """The whole pipeline runs, so a breakage shows up the day it happens."""
        result, _ = run("--run-on-days", "0")
        assert "Not a send day, not sending" in result.stderr
        assert result.stdout.startswith("Weekly report of modules with missing")
        assert "<table" in result.stdout

    def test_sends_on_matching_day(self, run):
        result, send = run("--run-on-days", "5")
        assert send.called
        assert "Sent report to" in result.stderr

    def test_sends_every_day_when_unset(self, run):
        _, send = run()
        assert send.called

    def test_dry_run_wins_on_a_send_day(self, run):
        """--dry-run still suppresses the send, and says why."""
        result, send = run("--run-on-days", "5", "--dry-run")
        assert not send.called
        assert "Dry run, not sending" in result.stderr


class TestRunDateDrivesEverything:
    """--date must fix the whole run, not just part of it.

    The deployed Spark job read the clock separately for the window, the expiry
    cutoff and the subject, so its output depended on when it ran. Here one date
    drives all three, which is what makes a run reproducible.
    """

    def test_window_subject_and_cutoff_all_use_run_date(self):
        fresh = [cli.Module(**vars(module)) for module in MODULES]
        patches = [
            mock.patch.object(cli, "query_modules", return_value=fresh),
            mock.patch.object(
                cli,
                "fetch_module_lists",
                return_value=(set(), {"xul.dll"}, set()),
            ),
            mock.patch.object(
                cli.symbols, "fetch_old_firefox_versions", return_value=set()
            ),
            mock.patch.object(
                cli.symbols, "are_symbols_available", return_value=False
            ),
            mock.patch.object(cli, "send_email"),
        ]
        with contextlib.ExitStack() as stack:
            mocks = [stack.enter_context(patch) for patch in patches]
            result = CliRunner().invoke(
                cli.main,
                ["--date", "2026-03-04", "--dry-run"],
                catch_exceptions=False,
            )
        query, _, cutoff, _, _ = mocks

        assert query.call_args.args[1] == datetime.date(2026, 3, 4)
        # Passed positionally as the `today` argument.
        assert cutoff.call_args.args[1].date() == datetime.date(2026, 3, 4)
        assert "2026-03-04" in result.stdout


class TestParityFlags:
    """The migration defaults to reproducing the Spark job, not correcting it."""

    def test_dedupe_key_defaults_to_spark_behaviour(self, run):
        _, send = run()
        assert cli.DEFAULT_DEDUPE_KEY == cli.DEDUPE_MODULE_STRUCT

    def test_dedupe_key_is_passed_to_the_query(self):
        with mock.patch.object(cli.bigquery, "Client") as client:
            client.return_value.query.return_value.result.return_value = []
            cli.query_modules("proj", "2026-08-06", 3, 70, cli.DEDUPE_CRASH_REPORT)
        params = {
            p.name: p.value
            for p in client.return_value.query.call_args.kwargs[
                "job_config"
            ].query_parameters
        }
        assert params["dedupe_key"] == cli.DEDUPE_CRASH_REPORT

    def test_query_defaults_to_module_struct_when_unspecified(self):
        with mock.patch.object(cli.bigquery, "Client") as client:
            client.return_value.query.return_value.result.return_value = []
            cli.query_modules("proj", "2026-08-06", 3, 70)
        params = {
            p.name: p.value
            for p in client.return_value.query.call_args.kwargs[
                "job_config"
            ].query_parameters
        }
        assert params["dedupe_key"] == cli.DEDUPE_MODULE_STRUCT

    def test_invalid_dedupe_key_rejected(self, run):
        result, send = run("--dedupe-key", "whatever")
        assert result.exit_code == 2
        assert not send.called

    def test_fix_availability_args_forwarded(self):
        """The flag has to reach are_symbols_available or it does nothing."""
        seen = []

        def fake(session, debug_file, debug_id, fix_arg_order=False):
            seen.append(fix_arg_order)
            return False

        for args, expected in ((), False), (("--fix-availability-args",), True):
            with mock.patch.object(cli.symbols, "are_symbols_available", fake):
                _run_with(args)
            assert seen and all(flag is expected for flag in seen), (
                f"{args} -> {seen}"
            )
            seen.clear()


def _run_with(extra_args):
    """Invoke main() with everything but are_symbols_available stubbed."""
    fresh = [cli.Module(**vars(module)) for module in MODULES]
    patches = [
        mock.patch.object(cli, "query_modules", return_value=fresh),
        mock.patch.object(
            cli,
            "fetch_module_lists",
            return_value=({"360netbase.dll"}, {"xul.dll"}, {"kernel32.dll"}),
        ),
        mock.patch.object(
            cli.symbols, "fetch_old_firefox_versions", return_value={"115"}
        ),
        mock.patch.object(cli, "send_email"),
    ]
    with contextlib.ExitStack() as stack:
        for patch in patches:
            stack.enter_context(patch)
        return CliRunner().invoke(
            cli.main,
            ["--date", "2026-08-07", *extra_args],
            catch_exceptions=False,
        )


class TestFiltering:
    def test_suppressed_modules_absent_from_email(self, run):
        result, _ = run("--dry-run")
        # known_modules entry and the expired Firefox version are both gone.
        assert "360NetBase.dll" not in result.stdout
        assert "115.0" not in result.stdout
        # The rest survive.
        assert "aswJsFlt.dll" in result.stdout
        assert "130.0" in result.stdout

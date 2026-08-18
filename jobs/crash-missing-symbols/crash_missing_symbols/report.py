"""Builds the HTML email body."""

from html import escape

CELL = 'style="border: 1px solid black;"'

# Wording kept as the old job had it, so the email reads the same to the people
# who get it weekly. Long lines are the email's own line breaks, not source
# formatting, so they're left alone.
FOOTER = """
The number of crash reports refers to the past {window_days} days.
Only modules with more than {min_crash_count:,} crash reports are shown in this list.

Firefox own modules, for which we should have symbols, and have the debug ID are colored in red.
For Firefox own modules, where we don't have a debug ID are colored in orange.
OS modules, for which we should have symbols, are colored in blue.

If you see modules that shouldn't be in this list as it's expected not
to have their symbols, either contact mcastelluccio@mozilla.com or open
a PR to add them to https://github.com/marco-c/missing_symbols/tree/master/known_modules.

The code that sends this email is at
https://github.com/mozilla/docker-etl/tree/main/jobs/crash-missing-symbols.
"""  # noqa: E501

AVAILABLE_NOTE = """
(*) We now have symbols for the modules marked with an asterisk. We could
reprocess them to improve stack traces (and maybe signatures) of some crash reports.

"""


def subject(report_date):
    return (
        "Weekly report of modules with missing symbols in crash reports: "
        f"{report_date.strftime('%Y-%m-%d')}"
    )


def _name_cell(module, firefox_modules, windows_modules):
    """Module name, colored by which list it belongs to."""
    name = escape(module.name)
    if module.name.lower() in firefox_modules:
        # Firefox modules we should have symbols for. Red when we have a debug
        # ID to look them up by, orange when we don't.
        color = "red" if module.debug_id else "orange"
        cell = f'<span style="color:{color};">{name}</span>'
    elif module.name.lower() in windows_modules:
        cell = f'<span style="color:blue;">{name}</span>'
    else:
        cell = name

    if module.symbols_available:
        cell += " (*)"
    return cell


def build_body(modules, firefox_modules, windows_modules, window_days,
               min_crash_count):
    """Render the report table plus the explanatory footer."""
    rows = [
        '<table style="border-collapse:collapse;">',
        "  <tr>",
        f"  <th {CELL}>Name</th>",
        f"  <th {CELL}>Version</th>",
        f"  <th {CELL}>Debug ID</th>",
        f"  <th {CELL}># of crash reports</th>",
        "</tr>",
    ]

    for module in modules:
        rows.append("<tr>")
        rows.append(
            f"<td {CELL}>"
            f"{_name_cell(module, firefox_modules, windows_modules)}</td>"
        )
        rows.append(f"<td {CELL}>{escape(module.version or '')}</td>")
        rows.append(f"<td {CELL}>{escape(module.debug_id or '')}</td>")
        rows.append(f"<td {CELL}>{module.crash_count:d}</td>")
        rows.append("</tr>")

    rows.append("</table>")

    body = "".join(rows) + "<pre>"
    if any(module.symbols_available for module in modules):
        body += AVAILABLE_NOTE
    body += FOOTER.format(
        window_days=window_days, min_crash_count=min_crash_count
    )
    return body + "</pre>"

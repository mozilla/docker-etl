import ast
import logging
import subprocess
import re

from datetime import date, datetime, timedelta

DAP_LEADER = "https://dap-09-3.api.divviup.org"
VDAF = "histogram"
PROCESS_TIMEOUT = 1200  # 20 mins


def get_aggregated_results(
    process_date: date,
    batch_start: date,
    batch_end: date,
    task_id: str,
    vdaf_length: int,
    collector_duration: int,
    bearer_token: str,
    hpke_config: str,
    hpke_private_key: str,
) -> dict:
    process_batch = _should_collect_batch(process_date, batch_end)

    if process_batch:
        # Step 4 Collect DAP results.
        aggregated_results = collect_dap_result(
            task_id=task_id,
            vdaf_length=vdaf_length,
            batch_start=batch_start,
            duration=collector_duration,
            bearer_token=bearer_token,
            hpke_config=hpke_config,
            hpke_private_key=hpke_private_key,
        )

        return aggregated_results


def current_batch_start(
    process_date: date, partner_start_date: date, duration: int
) -> date | None:
    if process_date < partner_start_date:
        return None

    if (
        partner_start_date
        <= process_date
        < partner_start_date + timedelta(seconds=duration)
    ):
        return partner_start_date

    # After the first interval ...
    batch_start = partner_start_date
    while True:
        next_start = batch_start + timedelta(seconds=duration)
        # check if the process_date is the batch_end date
        # if yes we only need to go back 1 duration to get the start
        if next_start + timedelta(days=-1) == process_date:
            return next_start + timedelta(seconds=-duration)

        # this means the process date is in the next interval so
        # need to go back 2 durations to get the batch_start
        if next_start > process_date:
            return next_start + timedelta(seconds=-2 * duration)

        batch_start = next_start


def current_batch_end(batch_start: date, duration: int) -> date:
    # since the start and end dates are inclusive need to subtract 1 from duration
    return batch_start + timedelta(seconds=duration, days=-1)


def _should_collect_batch(process_date, batch_end) -> bool:
    return batch_end == process_date


def _correct_wraparound(num: int) -> int:
    field_prime = 340282366920938462946865773367900766209
    field_size = 128
    cutoff = 2 ** (field_size - 1)
    if num > cutoff:
        return num - field_prime
    return num


def _parse_histogram(histogram_str: str) -> dict:
    parsed_list = ast.literal_eval(histogram_str)
    return {i: _correct_wraparound(val) for i, val in enumerate(parsed_list)}


def _parse_http_error(text: str) -> tuple[int, str, str | None] | None:
    """
    Returns (status_code, status_text, error_message)
    or None if the pattern is not found.
    """
    ERROR_RE = re.compile(
        r"HTTP response status\s+(\d+)\s+([A-Za-z ]+)(?:\s+-\s+(.*))?$"
    )
    match = ERROR_RE.search(text)
    if not match:
        return None

    status_code = int(match.group(1))
    status_text = match.group(2).strip()
    error_message = match.group(3).strip() if match.group(3) else None
    return status_code, status_text, error_message


# DAP functions
def collect_dap_result(
    task_id: str,
    vdaf_length: int,
    batch_start: date,
    duration: int,
    bearer_token: str,
    hpke_config: str,
    hpke_private_key: str,
) -> dict:
    # Beware! This command string reveals secrets. Use logging only for
    # debugging in local dev.

    batch_start_epoch = int(
        datetime.combine(batch_start, datetime.min.time()).timestamp()
    )

    try:
        result = subprocess.run(
            [
                "./collect",
                "--task-id",
                task_id,
                "--leader",
                DAP_LEADER,
                "--vdaf",
                VDAF,
                "--length",
                f"{vdaf_length}",
                "--authorization-bearer-token",
                bearer_token,
                "--batch-interval-start",
                f"{batch_start_epoch}",
                "--batch-interval-duration",
                f"{duration}",
                "--hpke-config",
                hpke_config,
                "--hpke-private-key",
                hpke_private_key,
            ],
            capture_output=True,
            text=True,
            check=True,
            timeout=PROCESS_TIMEOUT,
        )
        for line in result.stdout.splitlines():
            if line.startswith("Aggregation result:"):
                entries = _parse_histogram(line[21:-1])
                return entries
        # Beware! Exceptions thrown by the subprocess reveal secrets.
        # Log them and include traceback only for debugging in local dev.
    except subprocess.CalledProcessError as e:
        result = _parse_http_error(e.stderr)
        if result is None:
            logging.error(e)
            raise Exception(
                f"Collection failed for {task_id}, {e.returncode}, stderr: {e.stderr}"
            ) from None
        else:
            status_code, status_text, error_message = result
            if status_code == 400:
                logging.info(
                    f"Collection failed for {task_id}, {status_code} {status_text}"
                    f" {error_message}"
                )
            elif status_code == 404:
                detail = (
                    error_message
                    if error_message is not None
                    else "Verify start date is not more than 14 days ago."
                )
                logging.info(
                    f"Collection failed for {task_id}, {status_code} {status_text} "
                    f"{detail}"
                )
    except subprocess.TimeoutExpired as e:
        raise Exception(
            f"Collection timed out for {task_id}, {e.timeout}, stderr: {e.stderr}"
        ) from None

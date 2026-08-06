from datetime import UTC, datetime


def to_naive_datetime(input: datetime) -> str:
    return input.replace(tzinfo=None).isoformat()


def utc_from_naive_datetime(input: datetime) -> datetime:
    return input.replace(tzinfo=UTC)

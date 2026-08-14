import logging
import re
import time
from collections.abc import Iterator, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Optional

import httpx

Json = Mapping[str, "Json"] | Sequence["Json"] | str | int | float | bool | None


def get_json(url: str, headers: Optional[Mapping[str, str]] = None) -> Json:
    resp = httpx.get(url, headers=headers, follow_redirects=True)
    resp.raise_for_status()
    return resp.json()


@dataclass
class LinkHeader:
    prev: Optional[str] = None
    next: Optional[str] = None
    first: Optional[str] = None
    last: Optional[str] = None


def parse_link_header(link: Optional[str]) -> LinkHeader:
    rv = LinkHeader()
    if link is None:
        return rv

    rel_link = re.compile(r"""<(?P<url>[^>]+)>\s*;\s*rel=["']?(?P<rel>\w+)["']?\s*,?""")

    for item in rel_link.finditer(link):
        if item["rel"] in {"prev", "next", "first", "last"}:
            setattr(rv, item["rel"], item["url"])

    return rv


def get_paginated_json(
    url: str, headers: Optional[Mapping[str, str]] = None
) -> Sequence[Json]:
    data = []
    next_url: Optional[str] = url
    while next_url is not None:
        resp = httpx.get(next_url, headers=headers, follow_redirects=True)
        resp.raise_for_status()
        data.extend(resp.json())
        links = parse_link_header(resp.headers.get("link"))
        next_url = links.next
    return data


def retry_time(resp: httpx.Response) -> Optional[datetime]:
    """Return how long to wait before retrying a rate-limited response."""
    if resp.status_code not in (403, 429):
        return None

    retry_after = resp.headers.get("retry-after")
    if retry_after is not None:
        try:
            return datetime.now(tz=UTC) + timedelta(seconds=float(retry_after))
        except ValueError:
            pass

    if resp.headers.get("x-ratelimit-remaining") == "0":
        reset = resp.headers.get("x-ratelimit-reset")
        if reset is not None:
            try:
                # x-ratelimit-reset is a UTC epoch second value.
                return datetime.fromtimestamp(float(reset), tz=UTC)
            except ValueError:
                pass

    # Assume this is a non-rate-limit related 403/429
    return None


@dataclass
class PaginatedJsonResponse:
    data: Optional[Sequence[Json]]
    next_url: Optional[str]
    resume_at: Optional[datetime]


def iter_paginated_json(
    url: str, headers: Optional[Mapping[str, str]] = None
) -> Iterator[PaginatedJsonResponse]:
    next_url: Optional[str] = url
    while next_url is not None:
        resp = httpx.get(next_url, headers=headers, follow_redirects=True)

        resume_at = retry_time(resp)
        if resume_at is not None:
            yield PaginatedJsonResponse(
                data=None, next_url=next_url, resume_at=resume_at
            )
            # Add a small offset to reduce the chance of races
            sleep_seconds = resume_at.timestamp() - datetime.now(tz=UTC).timestamp() + 1
            if sleep_seconds > 0:
                logging.warning(
                    f"Rate limited fetching {next_url}; sleeping until {resume_at.isoformat()} ({sleep_seconds:.0f}s)"
                )
                time.sleep(sleep_seconds)
            continue

        resp.raise_for_status()
        links = parse_link_header(resp.headers.get("link"))
        next_url = links.next
        yield PaginatedJsonResponse(resp.json(), next_url, None)

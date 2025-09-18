import re
from dataclasses import dataclass
from typing import Mapping, Optional, Sequence

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


def get_paginated_json(url: str, headers: Optional[Mapping[str, str]] = None) -> Sequence[Json]:
    data = []
    next_url: Optional[str] = url
    while next_url is not None:
        resp = httpx.get(next_url, headers=headers, follow_redirects=True)
        resp.raise_for_status()
        data.extend(resp.json())
        links = parse_link_header(resp.headers.get("link"))
        next_url = links.next
    return data

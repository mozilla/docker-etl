from typing import Mapping, Optional, Sequence

import httpx

Json = Mapping[str, "Json"] | Sequence["Json"] | str | int | float | bool | None


def get_json(url: str, headers: Optional[Mapping[str, str]] = None) -> Json:
    resp = httpx.get(url, headers=headers, follow_redirects=True)
    resp.raise_for_status()
    return resp.json()

from os import environ
from functools import cache
from typing import Optional

import CloudFlare


def _account_id():
    return environ.get("CLOUDFLARE_ACCOUNT_ID")


@cache
def _get_client():
    _EMAIL = environ.get("CLOUDFLARE_EMAIL")
    _API_KEY = environ.get("CLOUDFLARE_API_KEY")

    return CloudFlare.CloudFlare(
        _EMAIL,
        _API_KEY,
    )


def domains_categories(domains: list[str]) -> list[dict]:
    results = _get_client().accounts.intel.domain.bulk(
        _account_id(), params={"domain": domains}
    )
    # for intel in results:
    #     print(intel)
    return [
        {
            "domain": intel["domain"],
            "categories": [
                {"id": c["id"], "parent_id": c["super_category_id"], "name": c["name"]}
                for c in intel.get("content_categories", [])
            ],
        }
        for intel in results
    ]


def _reverse_category_list(cats, acc: list = [], parent_id: Optional[int] = None):
    for cat in cats:
        acc.append(
            {"id": cat.get("id"), "parent_id": parent_id, "name": cat.get("name")}
        )
        if subs := cat.get("subcategories"):
            _reverse_category_list(subs, acc, cat.get("id"))
    return acc


def content_categories():
    return _reverse_category_list(
        _get_client().accounts.gateway.categories(_account_id())
    )

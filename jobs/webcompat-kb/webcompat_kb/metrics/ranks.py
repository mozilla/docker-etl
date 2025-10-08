import os
import tomllib
from typing import Mapping, Optional, Sequence

from pydantic import BaseModel, RootModel


class RankDataEntry(BaseModel):
    rank: Optional[str] = None
    crux_include: Optional[list[str]] = None
    crux_exclude: Optional[list[str]] = None


class RankData(RootModel):
    root: Mapping[str, RankDataEntry]


class RankColumn:
    def __init__(
        self,
        name: str,
        rank: Optional[str] = None,
        crux_include: Optional[list[str]] = None,
        crux_exclude: Optional[list[str]] = None,
    ):
        self.name = name
        self.rank = rank
        if crux_include or crux_exclude:
            if crux_include and crux_exclude:
                raise ValueError(
                    f"Rank definition {self.name} contains both crux_include and crux_exclude"
                )
            crux_countries = crux_include if crux_include else crux_exclude
            assert crux_countries is not None
            for item in crux_countries:
                if not (
                    item == "global"
                    or (len(item) == 2 and item.isascii() and item.islower())
                ):
                    raise ValueError(f"Invalid CrUX country code {item}")
            self.crux_condition = build_condition(
                "country_code", crux_countries, bool(crux_exclude)
            )
        else:
            self.crux_condition = None


def build_condition(
    field_name: str, items: Sequence[str], exclude: bool
) -> Optional[str]:
    if not items:
        return None
    if len(items) == 1:
        operator = "=" if not exclude else "!="
        return f'{field_name} {operator} "{items[0]}"'
    operator = "IN" if not exclude else "NOT IN"
    items_str = ", ".join(f'"{item}"' for item in items)
    return f"{field_name} {operator} UNNEST([{items_str}])"


_ranks: dict[str, Sequence[RankColumn]] = {}


def load(path: Optional[str] = None) -> Sequence[RankColumn]:
    if path is None:
        path = os.path.join(
            os.path.dirname(__file__),
            os.pardir,
            os.pardir,
            "data",
            "metrics",
            "ranks.toml",
        )
    path = os.path.abspath(path)
    if path not in _ranks:
        ranks = []
        with open(path, "rb") as f:
            data = tomllib.load(f)

        for name, rank_data in RankData.model_validate(data).root.items():
            ranks.append(
                RankColumn(
                    name=name,
                    rank=rank_data.rank,
                    crux_include=rank_data.crux_include or [],
                    crux_exclude=rank_data.crux_exclude or [],
                )
            )
        _ranks[path] = ranks

    return _ranks[path]

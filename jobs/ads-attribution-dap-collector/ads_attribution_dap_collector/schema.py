from datetime import date
from pydantic import BaseModel, Field, ConfigDict
from typing import Literal
from uuid import UUID


class CollectionConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")
    hpke_config: str = Field(min_length=1)


class Advertiser(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str = Field(min_length=1)
    partner_id: UUID
    start_date: date
    collector_duration: int = Field(gt=86399)  # 1 day - 1 sec in seconds
    conversion_type: Literal["view", "click", "default"]
    lookback_window: int = Field(gt=0)


class Partner(BaseModel):
    model_config = ConfigDict(extra="forbid")

    task_id: str = Field(min_length=32)
    vdaf: Literal["histogram", "sumvec", "sum"]
    bits: int | None = None
    length: int = Field(gt=0)
    time_precision: int
    default_measurement: int


class Ad(BaseModel):
    model_config = ConfigDict(extra="forbid")

    partner_id: UUID
    index: int


class JobConfig(BaseModel):
    """
    Full config with validation.
    Note: ads keys are dynamic (source:id), so they remain a dict[str, AdModel].
    """

    model_config = ConfigDict(extra="forbid")

    collection_config: CollectionConfig
    advertisers: list[Advertiser]
    partners: dict[UUID, Partner]
    ads: dict[str, Ad]

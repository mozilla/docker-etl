import logging
from datetime import date
from dataclasses import dataclass
from google.cloud import storage
import json
from typing import Any
from .schema import JobConfig
from pydantic import ValidationError
from uuid import UUID


CONFIG_FILE_NAME = "attribution-conf.json"


@dataclass(frozen=True)
class PartnerConfig:
    partner_id: UUID
    task_id: str
    vdaf: str
    bits: int
    length: int
    time_precision: int
    default_measurement: int


@dataclass(frozen=True)
class AdConfig:
    source: str
    ad_id: int
    index: int
    partner_id: UUID


@dataclass(frozen=True)
class AdvertiserConfig:
    name: str
    partner_id: UUID
    start_date: date
    collector_duration: int
    conversion_type: str
    lookback_window: int
    partner: PartnerConfig
    ads: list[AdConfig]


def get_config(gcp_project: str, config_bucket: str) -> dict[str, Any]:
    """Gets the attribution job's config from a file in a GCS bucket."""
    client = storage.Client(project=gcp_project)
    try:
        bucket = client.get_bucket(config_bucket)
        blob = bucket.blob(CONFIG_FILE_NAME)
        with blob.open("rt") as reader:
            config: dict[str, Any] = json.load(reader)
        return config
    except Exception as e:
        raise RuntimeError(
            f"Failed to get or parse job config file: {CONFIG_FILE_NAME} "
            f"from GCS bucket: {config_bucket} "
            f"in project: {gcp_project}."
        ) from e


def _require_ad_id_and_source(ad_key: str) -> tuple[str, int]:
    if ":" not in ad_key:
        raise ValueError(
            f"Skipping invalid ad key '{ad_key}': "
            f"missing ':' (expected 'source:id')"  # noqa: E231
        )

    source, ad_id_str = ad_key.split(":", 1)
    try:
        ad_id = int(ad_id_str)
    except ValueError:
        raise ValueError(
            f"Skipping invalid ad key '{ad_key}': ad_id '{ad_id_str}' is not an integer"
        )

    return source, ad_id


def extract_advertisers_with_partners_and_ads(
    raw_config: dict[str, Any]
) -> tuple[str, list[AdvertiserConfig]]:
    """
    Returns: (hpke_config, advertisers)
    - returns both hpke_config and advertisers.
    """

    try:
        cfg = JobConfig.model_validate(raw_config)
    except ValidationError as e:
        raise ValueError(f"Invalid config: {e}") from None

    hpke_config = cfg.collection_config.hpke_config

    # 1) Parse the leaf ads first and key by partnerId
    ads_by_partner: dict[UUID, list[AdConfig]] = {}
    for ad_key, ad in cfg.ads.items():
        try:
            source, ad_id = _require_ad_id_and_source(ad_key)
        except ValueError as e:
            logging.error(f"Skipping invalid ad key '{ad_key}':{e}")  # noqa: E231
            continue

        ad_cfg = AdConfig(
            source=source,
            ad_id=ad_id,
            index=ad.index,
            partner_id=ad.partner_id,
        )
        ads_by_partner.setdefault(ad.partner_id, []).append(ad_cfg)

    # 2) Add advertiser to partner+ads config
    out: list[AdvertiserConfig] = []

    for adv in cfg.advertisers:
        if adv.partner_id not in cfg.partners:
            raise ValueError(
                f"Advertiser '{adv.name}' references unknown partner_id "
                f"'{adv.partner_id}, available partners: {cfg.partners}'"
            )

        p = cfg.partners[adv.partner_id]

        partner = PartnerConfig(
            partner_id=adv.partner_id,
            task_id=p.task_id,
            vdaf=p.vdaf,
            bits=p.bits,
            length=p.length,
            time_precision=p.time_precision,
            default_measurement=p.default_measurement,
        )

        out.append(
            AdvertiserConfig(
                name=adv.name,
                partner_id=adv.partner_id,
                start_date=adv.start_date,
                collector_duration=adv.collector_duration,
                lookback_window=adv.lookback_window,
                conversion_type=adv.conversion_type,
                partner=partner,
                ads=ads_by_partner.get(adv.partner_id, []),
            )
        )

    return hpke_config, out

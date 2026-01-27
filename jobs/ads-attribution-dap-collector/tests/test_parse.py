import pytest
import re

from unittest import TestCase

from tests.test_mocks import (
    mock_get_valid_config,
    mock_get_config_invalid_conversion,
    mock_get_config_invalid_duration_value,
)

from ads_attribution_dap_collector.parse import (
    extract_advertisers_with_partners_and_ads,
    _require_ad_id_and_source,
)


class TestHelpers(TestCase):
    def test_require_ad_id_and_source(self):
        with self.assertRaises(ValueError):
            _require_ad_id_and_source("test_provider1234")

        source, ad_id = _require_ad_id_and_source("test_provider:1234abc")
        self.assertEqual("test_provider", source)
        self.assertEqual("1234abc", ad_id)

    def test_extract_advertisers_with_partners_and_ads(self):
        json_config = mock_get_valid_config()

        hpke_config, advertiser_configs = extract_advertisers_with_partners_and_ads(
            json_config
        )

        self.assertEqual(hpke_config, json_config["collection_config"]["hpke_config"])

        self.assertEqual(len(advertiser_configs), 2)

        for i, advertiser in enumerate(advertiser_configs):
            # check top level advertiser values
            self.assertEqual(advertiser.name, json_config["advertisers"][i]["name"])
            self.assertEqual(
                str(advertiser.partner_id), json_config["advertisers"][i]["partner_id"]
            )
            self.assertEqual(
                advertiser.start_date.isoformat(),
                json_config["advertisers"][i]["start_date"],
            )
            self.assertEqual(
                advertiser.collector_duration,
                json_config["advertisers"][i]["collector_duration"],
            )
            self.assertEqual(
                advertiser.conversion_type,
                json_config["advertisers"][i]["conversion_type"],
            )
            self.assertEqual(
                advertiser.lookback_window,
                json_config["advertisers"][i]["lookback_window"],
            )

            partner_config = advertiser.partner
            self.assertEqual(
                str(partner_config.partner_id),
                json_config["advertisers"][i]["partner_id"],
            )
            self.assertEqual(
                partner_config.task_id,
                json_config["partners"][json_config["advertisers"][i]["partner_id"]][
                    "task_id"
                ],
            )
            self.assertEqual(
                partner_config.vdaf,
                json_config["partners"][json_config["advertisers"][i]["partner_id"]][
                    "vdaf"
                ],
            )
            self.assertEqual(
                partner_config.bits,
                json_config["partners"][json_config["advertisers"][i]["partner_id"]][
                    "bits"
                ],
            )
            self.assertEqual(
                partner_config.length,
                json_config["partners"][json_config["advertisers"][i]["partner_id"]][
                    "length"
                ],
            )
            self.assertEqual(
                partner_config.time_precision,
                json_config["partners"][json_config["advertisers"][i]["partner_id"]][
                    "time_precision"
                ],
            )
            self.assertEqual(
                partner_config.default_measurement,
                json_config["partners"][json_config["advertisers"][i]["partner_id"]][
                    "default_measurement"
                ],
            )

            # check that the parsed ads match the json
            ads_config = advertiser.ads
            json_ads = json_config["ads"]
            for ad in ads_config:
                ad_key = f"{ad.source}:{ad.ad_id}"  # noqa: E231
                self.assertIn(ad_key, json_ads)
                json_ad = json_ads[ad_key]
                self.assertEqual(json_ad["index"], ad.index)
                self.assertEqual(
                    str(ad.partner_id), json_config["ads"][ad_key]["partner_id"]
                )

    def test_extract_advertisers_invalid_conversion(self):
        json_config = mock_get_config_invalid_conversion()
        with pytest.raises(
            Exception,
            match=re.escape("Input should be 'view', 'click' or 'default'"),
        ):
            extract_advertisers_with_partners_and_ads(json_config)

    def test_extract_advertisers_invalid_duration_value(self):
        json_config = mock_get_config_invalid_duration_value()
        with pytest.raises(
            Exception,
            match=re.escape("Input should be greater than 86399"),
        ):
            extract_advertisers_with_partners_and_ads(json_config)

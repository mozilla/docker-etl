from unittest import TestCase
from unittest.mock import patch
import json

import responses
from click.testing import CliRunner

import tests.fixtures.full_flight_test as full_test
from src import handler
from tests.fixtures.campaign_names import campaign_names
from tests.fixtures.flight_ads import (
    flight_11158378,
    flight_11322212,
    flight_8000_active_no_creatives,
    flight_8001_active_no_site_targeting,
    flight_8002_active_no_zone_targeting,
    flight_8003_active_no_targeting,
    flight_8004_active_non_standard_creative_template,
    flights as ad_flights,
)


class TestHandler(TestCase):

    @responses.activate
    def test_get_all_active_creative_maps(self):
        responses.add(responses.GET, handler.ADZERK_ALL_FLIGHTS, json=ad_flights)
        # 11322212 has 3 creative maps and 2 site/zone targeting = 6 ad objects
        responses.add(
            responses.GET,
            handler.ADZERK_FLIGHT.format("11322212"),
            json=flight_11322212,
        )
        # 11158378 has 3 creative maps and 1 site/zone targeting = 3 ad objects
        responses.add(
            responses.GET,
            handler.ADZERK_FLIGHT.format("11158378"),
            json=flight_11158378,
        )
        # 8000 has no creatives, 0 ad objects
        responses.add(
            responses.GET,
            handler.ADZERK_FLIGHT.format("8000"),
            json=flight_8000_active_no_creatives,
        )
        # 8001 has 3 creatives, 1 zone targeting, 3 ad objects
        responses.add(
            responses.GET,
            handler.ADZERK_FLIGHT.format("8001"),
            json=flight_8001_active_no_site_targeting,
        )
        # 8002 has 3 creatives, 1 site targeting, 3 ad objects
        responses.add(
            responses.GET,
            handler.ADZERK_FLIGHT.format("8002"),
            json=flight_8002_active_no_zone_targeting,
        )
        # 8003 has 3 creatives, no site/zone targeting, 3 ad objects
        responses.add(
            responses.GET,
            handler.ADZERK_FLIGHT.format("8003"),
            json=flight_8003_active_no_targeting,
        )
        # 8004 has 1 creative with a non-standard template, 1 site/zone targeting, 1 ad object
        responses.add(
            responses.GET,
            handler.ADZERK_FLIGHT.format("8004"),
            json=flight_8004_active_non_standard_creative_template,
        )
        # expect 19 objects
        ad_map = handler.get_all_active_creative_maps()
        # DEBUG
        self.assertEqual(19, len(ad_map))
        for ad in ad_map:
            # check that values were loaded correctly
            for key, value in ad.items():
                if ad["flight_id"] == 8001 and key == "site_id":
                    self.assertIsNone(value)
                elif ad["flight_id"] == 8002 and key == "zone_id":
                    self.assertIsNone(value)
                elif ad["flight_id"] == 8003 and (key == "site_id" or key == "zone_id"):
                    self.assertIsNone(value)
                elif ad["flight_id"] == 8004 and (
                    key == "sponsor" or key == "creative_title" or key == "creative_url"
                ):
                    self.assertIsNone(value)
                else:
                    self.assertIsNotNone(value)

    @responses.activate
    def test_get_campaign_name_map(self):
        responses.add(responses.GET, handler.ADZERK_ALL_CAMPAIGNS, json=campaign_names)
        expected_id_map = {
            843475: "Test Campaign (March)",
            887195: "Walmart (Sovrn) (Firefox)",
            887244: "Vrbo (Firefox)",
        }
        campaign_name_map = handler.get_campaign_name_map()
        self.assertEqual(3, len(campaign_name_map))
        for c_id, name in expected_id_map.items():
            self.assertEqual(name, campaign_name_map[c_id])

    def test_decorate_with_info_using_ad_value(self):
        ad_template = {
            "campaign_id": 2000,
            "ad_id": 3000,
        }
        ads_ = []
        for i in range(5):
            ad = ad_template.copy()
            ad["campaign_id"] += i

            ads_.append(ad)

        campaign_name_map = {
            2000: "c0",
            2001: "c1",
            2002: "c2",
            2003: "c3",
            2004: "c4",
        }

        actual_ads = handler.decorate_with_info(
            ads_, campaign_name_map, "campaign_name", "campaign_id"
        )
        for i in range(5):
            self.assertEqual("c" + str(i), actual_ads[i]["campaign_name"])

    def test_decorate_with_info_using_named_key(self):
        ad_template = {"ad_id": 1, "rate_type": 0}
        ads_ = []
        for i in range(5):
            ad = ad_template.copy()
            ad["rate_type"] += i

            ads_.append(ad)

        rates = {
            0: "rate type 1",
            1: "rate type 2",
            2: "rate type 3",
            3: "rate type 4",
            4: "rate type 5",
        }

        for i in range(5):
            # copy the field "i" from rates to "rate_type" in the ad
            modified_ad = handler.decorate_with_info(
                [ads_[i]], rates, "rate_type", i, ad_value=False
            )
            self.assertEqual(rates[i], modified_ad[0]["rate_type"])
    @responses.activate
    def test_main_handler_ads(self):
        # set up responses
        responses.add(
            responses.GET, handler.ADZERK_ALL_FLIGHTS, json=full_test.all_flights_1
        )
        responses.add(
            responses.GET,
            handler.ADZERK_FLIGHT.format(str(8144015)),
            json=full_test.flight_1,
        )
        responses.add(
            responses.GET, handler.ADZERK_ALL_CAMPAIGNS, json=full_test.campaign_1
        )
        responses.add(responses.GET, handler.ADZERK_ZONES, json=full_test.zone_1)
        responses.add(responses.GET, handler.ADZERK_SITES, json=full_test.site_1)
        test_ads = []
        with patch("src.handler.bigquery"):
            with patch("src.handler.storage"):
                runner = CliRunner()
                result = runner.invoke(handler.main, ["--project", "test", "--bucket", "test", "--api-key", "test", "--env", "dev", ])
                assert result.exit_code == 0
                with open("/tmp/batch.json") as f:
                    ads = f.readlines()
                    for a in ads:
                        test_ads.append(json.loads(a))
        ads_simple = test_ads
        self.assertEqual(4, len(ads_simple))
        ad_10108517 = [x for x in ads_simple if x["ad_id"] == 10108517]
        ad_12010338 = [x for x in ads_simple if x["ad_id"] == 12010338]

        ad_nests = [ad_12010338, ad_10108517]

        for ad_type in ad_nests:
            for ad in ad_type:
                if ad["site_id"] == 1082659:
                    self.assertEqual("Unit test production", ad["site_name"])
                elif ad["site_id"] == 10826560:
                    self.assertEqual("Unit test staging", ad["site_name"])
                else:
                    raise AssertionError(
                        "Site id to name mapping is wrong for ad id {}".format(
                            ad["ad_id"]
                        )
                    )

                if ad["zone_id"] == 204604:
                    self.assertEqual("BestZone", ad["zone_name"])
                elif ad["zone_id"] == 204605:
                    self.assertEqual("SecondBestZone", ad["zone_name"])
                else:
                    raise AssertionError(
                        "Zone id to name mapping is wrong for ad id {}".format(
                            ad["ad_id"]
                        )
                    )

                if ad["campaign_id"] == 838705:
                    self.assertEqual("My Test Campaign", ad["campaign_name"])
                else:
                    raise AssertionError(
                        "campaign id to name mapping is wrong for ad id {}".format(
                            ad["ad_id"]
                        )
                    )

        for ad in ads_simple:
            self.assertTrue("ad_id" in ad)
            self.assertTrue("creative_id" in ad)
            self.assertTrue("campaign_id" in ad)
            self.assertTrue("campaign_name" in ad)
            self.assertTrue("sponsor" in ad)
            self.assertTrue("advertiser_id" in ad)
            self.assertTrue("creative_title" in ad)
            self.assertTrue("creative_url" in ad)
            self.assertTrue("content_url" in ad)
            self.assertTrue("image_url" in ad)
            self.assertTrue("flight_id" in ad)
            self.assertTrue("flight_name" in ad)
            self.assertTrue("rate_type" in ad)
            self.assertTrue("price" in ad)
            self.assertTrue("site_name" in ad)
            self.assertTrue("zone_name" in ad)
            self.assertTrue("site_id" in ad)
            self.assertTrue("zone_id" in ad)

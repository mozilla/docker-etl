REFERRER_MEASUREMENT_EXPERIMENT_SUCCESS = {
    "slug": "interesting-study-5",
    "appName": "firefox_desktop",
    "appId": "firefox-desktop",
    "channel": "nightly",
    "bucketConfig": {
        "randomizationUnit": "group_id",
        "namespace": "firefox-desktop-dapIncrementality-newtabSponsoredContent-nightly-group_id-2",
        "start": 0,
        "count": 10000,
        "total": 10000,
    },
    "featureIds": ["dapIncrementality", "newtabSponsoredContent"],
    "branches": [
        {
            "slug": "control",
            "ratio": 1,
            "features": [
                {
                    "featureId": "dapIncrementality",
                    "enabled": True,
                    "value": {
                        "advertiser": "Bookshop",
                        "taskId": "0QqFBHvuEk1_y4v4GIa9bTaa3vXXtLjsK64QeifzHpo",
                        "length": 4,
                        "timePrecision": 3600,
                        "measurementType": "referrerMeasurement",
                        "referrerUrls": [
                            {
                                "url": "*://*.bookshop.com/",
                                "bucket": 1,
                                "metric_name": "organic_conversions",
                            }
                        ],
                        "targetUrls": "*://*.bookshop.com/thankyou.html?*orderId*",
                        "unknownReferrerBucket": 1,
                    },
                },
                {
                    "featureId": "newtabSponsoredContent",
                    "enabled": True,
                    "value": {
                        "tilesPlacements": "newtab_tile_exp_1a, newtab_tile_exp_2a, newtab_tile_exp_3a"
                    },
                },
            ],
            "firefoxLabsTitle": None,
        },
        {
            "slug": "treatment-a",
            "ratio": 1,
            "features": [
                {
                    "featureId": "dapIncrementality",
                    "enabled": True,
                    "value": {
                        "advertiser": "Bookshop",
                        "taskId": "0QqFBHvuEk1_y4v4GIa9bTaa3vXXtLjsK64QeifzHpo",
                        "length": 4,
                        "timePrecision": 3600,
                        "measurementType": "referrerMeasurement",
                        "referrerUrls": [
                            {
                                "url": "*://*.bookshop.com/",
                                "bucket": 2,
                                "metric_name": "organic_conversions_tile_pinned",
                            }
                        ],
                        "targetUrls": "*://*.bookshop.com/thankyou.html?*orderId*",
                        "unknownReferrerBucket": 2,
                    },
                },
                {
                    "featureId": "newtabSponsoredContent",
                    "enabled": True,
                    "value": {
                        "tilesPlacements": "newtab_tile_exp_1b, newtab_tile_exp_2a, newtab_tile_exp_3a"
                    },
                },
            ],
            "firefoxLabsTitle": None,
        },
        {
            "slug": "treatment-b",
            "ratio": 1,
            "features": [
                {
                    "featureId": "dapIncrementality",
                    "enabled": True,
                    "value": {
                        "advertiser": "Bookshop",
                        "taskId": "0QqFBHvuEk1_y4v4GIa9bTaa3vXXtLjsK64QeifzHpo",
                        "length": 4,
                        "timePrecision": 3600,
                        "measurementType": "referrerMeasurement",
                        "referrerUrls": [
                            {
                                "url": "*://*.bookshop.com/*?tag=partnerus-20*ref=*mfadid=partner",
                                "bucket": 3,
                                "metric_name": "total_conversions_tile_pinned",
                            },
                            {
                                "url": "*://*.bookshop.com/",
                                "bucket": 3,
                                "metric_name": "total_conversions_tile_pinned",
                            },
                        ],
                        "targetUrls": "*://*.bookshop.com/thankyou.html?*orderId*",
                        "unknownReferrerBucket": 3,
                    },
                },
                {
                    "featureId": "newtabSponsoredContent",
                    "enabled": True,
                    "value": {
                        "tilesPlacements": "newtab_tile_exp_1b, newtab_tile_exp_2a, newtab_tile_exp_3a"
                    },
                },
            ],
            "firefoxLabsTitle": None,
        },
    ],
    # noqa: E501
    "targeting": "(browserSettings.update.channel == \"nightly\") && ((experiment.slug in activeExperiments) || ((\n        'browser.newtabpage.activity-stream.showSponsoredTopSites'|preferenceValue\n    ) && (version|versionCompare('140.!') >= 0) && (region in ['US'])))",  # noqa: E501
    "startDate": "2025-08-18",
    "endDate": None,
    "proposedEnrollment": 7,
    "referenceBranch": "control",
}

VISIT_MEASUREMENT_EXPERIMENT_SUCCESS = {
    "slug": "interesting-study-6",
    "appName": "firefox_desktop",
    "appId": "firefox-desktop",
    "channel": "nightly",
    "bucketConfig": {
        "randomizationUnit": "group_id",
        "namespace": "firefox-desktop-dapIncrementality-newtabSponsoredContent-nightly-group_id-2",
        "start": 0,
        "count": 10000,
        "total": 10000,
    },
    "featureIds": ["dapIncrementality", "newtabSponsoredContent"],
    "branches": [
        {
            "slug": "control",
            "ratio": 1,
            "features": [
                {
                    "featureId": "dapIncrementality",
                    "enabled": True,
                    "value": {
                        "advertiser": "Fashion Brand",
                        "taskId": "JASGxjh2Fptfv6gsSFpczwBcAib5oxaI-KPUqb7sHfs",
                        "length": 4,
                        "timePrecision": 3600,
                        "measurementType": "visitMeasurement",
                        "visitCountUrls": [
                            {
                                "url": "*://*.fashion-brand.com/",
                                "bucket": 1,
                                "metric_name": "organic_visits_tile_blocked",
                            }
                        ],
                        "unknownReferrerBucket": 1,
                    },
                },
                {
                    "featureId": "newtabSponsoredContent",
                    "enabled": True,
                    "value": {
                        "tilesPlacements": "newtab_tile_exp_1a, newtab_tile_exp_2a, newtab_tile_exp_3a"
                    },
                },
            ],
            "firefoxLabsTitle": None,
        },
        {
            "slug": "treatment-a",
            "ratio": 1,
            "features": [
                {
                    "featureId": "dapIncrementality",
                    "enabled": True,
                    "value": {
                        "advertiser": "Fashion Brand",
                        "taskId": "JASGxjh2Fptfv6gsSFpczwBcAib5oxaI-KPUqb7sHfs",
                        "length": 4,
                        "timePrecision": 3600,
                        "measurementType": "visitMeasurement",
                        "visitCountUrls": [
                            {
                                "url": "*://*.fashion-brand.com/",
                                "bucket": 2,
                                "metric_name": "organic_visits_tile_pinned",
                            }
                        ],
                        "unknownReferrerBucket": 2,
                    },
                },
                {
                    "featureId": "newtabSponsoredContent",
                    "enabled": True,
                    "value": {
                        "tilesPlacements": "newtab_tile_exp_1b, newtab_tile_exp_2a, newtab_tile_exp_3a"
                    },
                },
            ],
            "firefoxLabsTitle": None,
        },
        {
            "slug": "treatment-b",
            "ratio": 1,
            "features": [
                {
                    "featureId": "dapIncrementality",
                    "enabled": True,
                    "value": {
                        "advertiser": "Fashion Brand",
                        "taskId": "JASGxjh2Fptfv6gsSFpczwBcAib5oxaI-KPUqb7sHfs",
                        "length": 4,
                        "timePrecision": 3600,
                        "measurementType": "visitMeasurement",
                        "visitCountUrls": [
                            {
                                "url": "*://*.fashion-brand.com/*?tag=partnerus-20*ref=*mfadid=partner",
                                "bucket": 3,
                                "metric_name": "total_visits_tile_pinned",
                            },
                            {
                                "url": "*://*.fashion-brand.com/",
                                "bucket": 3,
                                "metric_name": "total_visits_tile_pinned",
                            },
                        ],
                        "unknownReferrerBucket": 3,
                    },
                },
                {
                    "featureId": "newtabSponsoredContent",
                    "enabled": True,
                    "value": {
                        "tilesPlacements": "newtab_tile_exp_1b, newtab_tile_exp_2a, newtab_tile_exp_3a"
                    },
                },
            ],
            "firefoxLabsTitle": None,
        },
    ],
    # noqa: E501
    "targeting": "(browserSettings.update.channel == \"nightly\") && ((experiment.slug in activeExperiments) || ((\n        'browser.newtabpage.activity-stream.showSponsoredTopSites'|preferenceValue\n    ) && (version|versionCompare('140.!') >= 0) && (region in ['US'])))",  # noqa: E501
    "startDate": "2025-08-18",
    "endDate": None,
    "proposedEnrollment": 7,
    "referenceBranch": "control",
}

UNKOWN_MEASUREMENT_EXPERIMENT_SUCCESS = {
    "slug": "interesting-study-7",
    "appName": "firefox_desktop",
    "appId": "firefox-desktop",
    "channel": "nightly",
    "bucketConfig": {
        "randomizationUnit": "group_id",
        "namespace": "firefox-desktop-dapIncrementality-newtabSponsoredContent-nightly-group_id-2",
        "start": 0,
        "count": 10000,
        "total": 10000,
    },
    "featureIds": ["dapIncrementality", "newtabSponsoredContent"],
    "branches": [
        {
            "slug": "control",
            "ratio": 1,
            "features": [
                {
                    "featureId": "dapIncrementality",
                    "enabled": True,
                    "value": {
                        "advertiser": "Fashion Brand",
                        "taskId": "JASGxjh2Fptfv6gsSFpczwBcAib5oxaI-KPUqb7sHfs",
                        "length": 4,
                        "timePrecision": 3600,
                        "measurementType": "someUnknownMeasurementType",
                        "visitCountUrls": [
                            {
                                "url": "*://*.fashion-brand.com/",
                                "bucket": 1,
                                "metric_name": "organic_visits_tile_blocked",
                            }
                        ],
                        "unknownReferrerBucket": 1,
                    },
                },
                {
                    "featureId": "newtabSponsoredContent",
                    "enabled": True,
                    "value": {
                        "tilesPlacements": "newtab_tile_exp_1a, newtab_tile_exp_2a, newtab_tile_exp_3a"
                    },
                },
            ],
            "firefoxLabsTitle": None,
        },
        {
            "slug": "treatment-a",
            "ratio": 1,
            "features": [
                {
                    "featureId": "dapIncrementality",
                    "enabled": True,
                    "value": {
                        "advertiser": "Fashion Brand",
                        "taskId": "JASGxjh2Fptfv6gsSFpczwBcAib5oxaI-KPUqb7sHfs",
                        "length": 4,
                        "timePrecision": 3600,
                        "measurementType": "someUnknownMeasurementType",
                        "visitCountUrls": [
                            {
                                "url": "*://*.fashion-brand.com/",
                                "bucket": 2,
                                "metric_name": "organic_visits_tile_pinned",
                            }
                        ],
                        "unknownReferrerBucket": 2,
                    },
                },
                {
                    "featureId": "newtabSponsoredContent",
                    "enabled": True,
                    "value": {
                        "tilesPlacements": "newtab_tile_exp_1b, newtab_tile_exp_2a, newtab_tile_exp_3a"
                    },
                },
            ],
            "firefoxLabsTitle": None,
        },
    ],
    # noqa: E501
    "targeting": "(browserSettings.update.channel == \"nightly\") && ((experiment.slug in activeExperiments) || ((\n        'browser.newtabpage.activity-stream.showSponsoredTopSites'|preferenceValue\n    ) && (version|versionCompare('140.!') >= 0) && (region in ['US'])))",  # noqa: E501
    "startDate": "2025-08-18",
    "endDate": None,
    "proposedEnrollment": 7,
    "referenceBranch": "control",
}


NOT_AN_INCREMENTALITY_EXPERIMENT_SUCCESS = {
    "slug": "something-experiment",
    "appName": "firefox_desktop",
    "appId": "firefox-desktop",
    "channel": "nightly",
    "bucketConfig": {
        "randomizationUnit": "group_id",
        "namespace": "firefox-desktop-dapIncrementality-newtabSponsoredContent-nightly-group_id-2",
        "start": 0,
        "count": 10000,
        "total": 10000,
    },
    "featureIds": [
        "dapIncrementalityIsNotPartOfThisExperiment",
        "newtabSponsoredContentIsNotPartOfThisExperiment",
    ],
    "branches": [
        {
            "slug": "control",
            "ratio": 1,
            "features": [
                {
                    "featureId": "dapIncrementalityIsNotPartOfThisExperiment",
                    "enabled": True,
                    "value": {
                        "enabled": True,
                        "somethingEnabled": True,
                        "someOtherExperimentList": [
                            {
                                "name": "1841986890",
                            }
                        ],
                    },
                },
                {
                    "featureId": "newtabSponsoredContentIsNotPartofThisExperiment",
                    "enabled": True,
                    "value": {
                        "somePlacements": "something_1a, something_2a, something_3a"
                    },
                },
            ],
            "firefoxLabsTitle": None,
        },
        {
            "slug": "treatment-a",
            "ratio": 1,
            "features": [
                {
                    "featureId": "dapIncrementalityIsNotPartOfThisExperiment",
                    "enabled": True,
                    "value": {
                        "enabled": True,
                        "somethingEnabled": True,
                        "someOtherExperimentList": [
                            {
                                "name": "1841986625495",
                            }
                        ],
                    },
                },
                {
                    "featureId": "newtabSponsoredContentIsNotPartofThisExperiment",
                    "enabled": True,
                    "value": {
                        "somePlacements": "something_1a, something_2a, something_3a"
                    },
                },
            ],
            "firefoxLabsTitle": None,
        },
        {
            "slug": "treatment-b",
            "ratio": 1,
            "features": [
                {
                    "featureId": "dapIncrementalityIsNotPartOfThisExperiment",
                    "enabled": True,
                    "value": {
                        "enabled": True,
                        "somethingEnabled": True,
                        "someOtherExperimentList": [
                            {
                                "name": "18419866254958234765",
                            }
                        ],
                    },
                },
                {
                    "featureId": "newtabSponsoredContentIsNotPartofThisExperiment",
                    "enabled": True,
                    "value": {
                        "somePlacements": "something_1a, something_2a, something_3a"
                    },
                },
            ],
            "firefoxLabsTitle": None,
        },
    ],
    "targeting": "(browserSettings.update.channel == \"nightly\") && ((experiment.slug in activeExperiments) || ((\n        'browser.newtabpage.activity-stream.showSponsoredTopSites'|preferenceValue\n    ) && (version|versionCompare('140.!') >= 0) && (region in ['US'])))",  # noqa: E501
    "startDate": "2025-08-18",
    "endDate": "2025-09-15",
    "proposedEnrollment": 7,
    "referenceBranch": "control",
}

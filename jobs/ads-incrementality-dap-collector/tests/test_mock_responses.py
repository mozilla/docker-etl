REFERRER_MEASUREMENT_EXPERIMENT_SUCCESS = {
    "slug": "interesting-study-5",
    "appName": "firefox_desktop",
    "appId": "firefox-desktop",
    "channel": "nightly",
    "featureIds": ["dapIncrementality"],
    "branches": [
        {
            "slug": "control",
            "ratio": 1,
            "features": [
                {
                    "featureId": "dapIncrementality",
                    "enabled": True,
                    "value": {
                        "advertiser": "Example Shop",
                        "taskId": "0QqFBHvuEk1_y4v4GIa9bTaa3vXXtLjsK64QeifzHpo",
                        "length": 4,
                        "timePrecision": 3600,
                        "measurementType": "referrerMeasurement",
                        "referrerUrls": [
                            {
                                "url": "*://*.example-book-store.com/",
                                "bucket": 1,
                                "metric_name": "some_metric",
                            }
                        ],
                        "targetUrls": "*://*.example-book-store.com/page.html",
                        "unknownReferrerBucket": 1,
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
                        "advertiser": "Example Shop",
                        "taskId": "0QqFBHvuEk1_y4v4GIa9bTaa3vXXtLjsK64QeifzHpo",
                        "length": 4,
                        "timePrecision": 3600,
                        "measurementType": "referrerMeasurement",
                        "referrerUrls": [
                            {
                                "url": "*://*.example-book-store.com/",
                                "bucket": 2,
                                "metric_name": "some_metric",
                            }
                        ],
                        "targetUrls": "*://*.example-book-store.com/page.html",
                        "unknownReferrerBucket": 2,
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
                        "advertiser": "Example Shop",
                        "taskId": "0QqFBHvuEk1_y4v4GIa9bTaa3vXXtLjsK64QeifzHpo",
                        "length": 4,
                        "timePrecision": 3600,
                        "measurementType": "referrerMeasurement",
                        "referrerUrls": [
                            {
                                "url": "*://*.example-book-store.com/page.html",
                                "bucket": 3,
                                "metric_name": "some_metric",
                            },
                            {
                                "url": "*://*.example-book-store.com/",
                                "bucket": 3,
                                "metric_name": "some_metric",
                            },
                        ],
                        "targetUrls": "*://*.example-book-store.com/page.html",
                        "unknownReferrerBucket": 3,
                    },
                },
            ],
            "firefoxLabsTitle": None,
        },
    ],
    "targeting": '(browserSettings.update.channel == "nightly") && '
    "((experiment.slug in activeExperiments) || "
    "((\n        'browser.newtabpage.activity-stream.showSponsoredTopSites'|preferenceValue\n    ) && "
    "(version|versionCompare('140.!') >= 0) && (region in ['US'])))",
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
    "featureIds": ["dapIncrementality"],
    "branches": [
        {
            "slug": "control",
            "ratio": 1,
            "features": [
                {
                    "featureId": "dapIncrementality",
                    "enabled": True,
                    "value": {
                        "advertiser": "Example Brand",
                        "taskId": "JASGxjh2Fptfv6gsSFpczwBcAib5oxaI-KPUqb7sHfs",
                        "length": 4,
                        "timePrecision": 3600,
                        "measurementType": "visitMeasurement",
                        "visitCountUrls": [
                            {
                                "url": "*://*.example-brand.com/",
                                "bucket": 1,
                                "metric_name": "another_metric",
                            }
                        ],
                        "unknownReferrerBucket": 1,
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
                        "advertiser": "Example Brand",
                        "taskId": "JASGxjh2Fptfv6gsSFpczwBcAib5oxaI-KPUqb7sHfs",
                        "length": 4,
                        "timePrecision": 3600,
                        "measurementType": "visitMeasurement",
                        "visitCountUrls": [
                            {
                                "url": "*://*.example-brand.com/",
                                "bucket": 2,
                                "metric_name": "another_metric",
                            }
                        ],
                        "unknownReferrerBucket": 2,
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
                        "advertiser": "Example Brand",
                        "taskId": "JASGxjh2Fptfv6gsSFpczwBcAib5oxaI-KPUqb7sHfs",
                        "length": 4,
                        "timePrecision": 3600,
                        "measurementType": "visitMeasurement",
                        "visitCountUrls": [
                            {
                                "url": "*://*.example-brand.com/page.html",
                                "bucket": 3,
                                "metric_name": "another_metric",
                            },
                            {
                                "url": "*://*.example-brand.com/",
                                "bucket": 3,
                                "metric_name": "another_metric",
                            },
                        ],
                        "unknownReferrerBucket": 3,
                    },
                },
            ],
            "firefoxLabsTitle": None,
        },
    ],
    "targeting": '(browserSettings.update.channel == "nightly") && '
    "((experiment.slug in activeExperiments) || "
    "((\n        'browser.newtabpage.activity-stream.showSponsoredTopSites'|preferenceValue\n    ) && "
    "(version|versionCompare('140.!') >= 0) && (region in ['US'])))",
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
    "featureIds": ["dapIncrementality"],
    "branches": [
        {
            "slug": "control",
            "ratio": 1,
            "features": [
                {
                    "featureId": "dapIncrementality",
                    "enabled": True,
                    "value": {
                        "advertiser": "Example Brand",
                        "taskId": "JASGxjh2Fptfv6gsSFpczwBcAib5oxaI-KPUqb7sHfs",
                        "length": 4,
                        "timePrecision": 3600,
                        "measurementType": "someUnknownMeasurementType",
                        "visitCountUrls": [
                            {
                                "url": "*://*.example-brand.com/",
                                "bucket": 1,
                                "metric_name": "some_metric",
                            }
                        ],
                        "unknownReferrerBucket": 1,
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
                        "advertiser": "Example Brand",
                        "taskId": "JASGxjh2Fptfv6gsSFpczwBcAib5oxaI-KPUqb7sHfs",
                        "length": 4,
                        "timePrecision": 3600,
                        "measurementType": "someUnknownMeasurementType",
                        "visitCountUrls": [
                            {
                                "url": "*://*.example-brand.com/",
                                "bucket": 2,
                                "metric_name": "some_metric",
                            }
                        ],
                        "unknownReferrerBucket": 2,
                    },
                },
            ],
            "firefoxLabsTitle": None,
        },
    ],
    "targeting": '(browserSettings.update.channel == "nightly") && '
    "((experiment.slug in activeExperiments) || "
    "((\n        'browser.newtabpage.activity-stream.showSponsoredTopSites'|preferenceValue\n    ) && "
    "(version|versionCompare('140.!') >= 0) && (region in ['US'])))",
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
    "featureIds": [
        "dapIncrementalityIsNotPartOfThisExperiment",
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
            ],
            "firefoxLabsTitle": None,
        },
    ],
    "targeting": '(browserSettings.update.channel == "nightly") && '
    "((experiment.slug in activeExperiments) || "
    "((\n        'browser.newtabpage.activity-stream.showSponsoredTopSites'|preferenceValue\n    ) && "
    "(version|versionCompare('140.!') >= 0) && (region in ['US'])))",
    "startDate": "2025-08-18",
    "endDate": "2025-09-15",
    "proposedEnrollment": 7,
    "referenceBranch": "control",
}

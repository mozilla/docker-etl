NIMBUS_SUCCESS = {
    'slug': 'traffic-impact-study-5',
    'appName': 'firefox_desktop',
    'appId': 'firefox-desktop',
    'channel': 'nightly',
    'bucketConfig': {
        'randomizationUnit': 'group_id',
        'namespace': 'firefox-desktop-dapTelemetry-newtabSponsoredContent-nightly-group_id-2',
        'start': 0,
        'count': 10000,
        'total': 10000
    },
    'featureIds': ['dapTelemetry', 'newtabSponsoredContent'],
    'branches': [
        {
            'slug': 'control',
            'ratio': 1,
            'features': [
                {
                    'featureId': 'dapTelemetry',
                    'enabled': True,
                    'value': {
                        'enabled': True,
                        'visitCountingEnabled': True,
                        'visitCountingExperimentList': [
                            {
                                'name': '1841986',
                                'bucket': 1,
                                'task_id': 'mubArkO3So8Co1X98CBo62-lSCM4tB-NZPOUGJ83N1o',
                                'task_veclen': 4,
                                'urls': ['*://*.glamazon.com/']
                            }
                        ]
                    }
                },
                {
                    'featureId': 'newtabSponsoredContent',
                    'enabled': True,
                    'value': {
                        'tilesPlacements': 'newtab_tile_exp_1a, newtab_tile_exp_2a, newtab_tile_exp_3a'
                    }
                }
            ],
            'firefoxLabsTitle': None
        },
        {
            'slug': 'treatment-a',
            'ratio': 1,
            'features': [
                {
                    'featureId': 'dapTelemetry',
                    'enabled': True,
                    'value': {
                        'enabled': True,
                        'visitCountingEnabled': True,
                        'visitCountingExperimentList': [
                            {
                                'name': '1841986',
                                'bucket': 2,
                                'task_id': 'mubArkO3So8Co1X98CBo62-lSCM4tB-NZPOUGJ83N1o',
                                'task_veclen': 4,
                                'urls': ['*://*.glamazon.com/']
                            }
                        ]
                    }
                },
                {
                    'featureId': 'newtabSponsoredContent',
                    'enabled': True,
                    'value': {
                        'tilesPlacements': 'newtab_tile_exp_1b, newtab_tile_exp_2a, newtab_tile_exp_3a'
                    }
                }
            ],
            'firefoxLabsTitle': None
        },
        {
            'slug': 'treatment-b',
            'ratio': 1,
            'features': [
                {
                    'featureId': 'dapTelemetry',
                    'enabled': True,
                    'value': {
                        'enabled': True,
                        'visitCountingEnabled': True,
                        'visitCountingExperimentList': [
                            {
                                'name': '1841986',
                                'bucket': 3,
                                'task_id': 'mubArkO3So8Co1X98CBo62-lSCM4tB-NZPOUGJ83N1o',
                                'task_veclen': 4,
                                'urls': ['*://*.glamazon.com/', '*://*.glamazon.com/*tag=admarketus*ref=*mfadid=adm']
                            }
                        ]
                    }
                },
                {
                    'featureId': 'newtabSponsoredContent',
                    'enabled': True,
                    'value': {
                        'tilesPlacements': 'newtab_tile_exp_1b, newtab_tile_exp_2a, newtab_tile_exp_3a'}
                }
            ],
            'firefoxLabsTitle': None
        }
    ],
    'targeting': '(browserSettings.update.channel == "nightly") && ((experiment.slug in activeExperiments) || ((\n        \'browser.newtabpage.activity-stream.showSponsoredTopSites\'|preferenceValue\n    ) && (version|versionCompare(\'140.!\') >= 0) && (region in [\'US\'])))',
    'startDate': '2025-08-18',
    'enrollmentEndDate': None,
    'endDate': None,
    'proposedEnrollment': 7,
    'referenceBranch': 'control',
}

NIMBUS_NOT_AN_INCREMENTALITY_EXPERIMENT = {
    'slug': 'something-experiment',
    'appName': 'firefox_desktop',
    'appId': 'firefox-desktop',
    'channel': 'nightly',
    'bucketConfig': {
        'randomizationUnit': 'group_id',
        'namespace': 'firefox-desktop-dapTelemetry-newtabSponsoredContent-nightly-group_id-2',
        'start': 0,
        'count': 10000,
        'total': 10000
    },
    'featureIds': ['dapTelemetryIsNotPartOfThisExperiment', 'newtabSponsoredContentIsNotPartOfThisExperiment'],
    'branches': [
        {
            'slug': 'control',
            'ratio': 1,
            'features': [
                {
                    'featureId': 'dapTelemetryIsNotPartOfThisExperiment',
                    'enabled': True,
                    'value': {
                        'enabled': True,
                        'somethingEnabled': True,
                        'someOtherExperimentList': [
                            {
                                'name': '1841986890',
                            }
                        ]
                    }
                },
                {
                    'featureId': 'newtabSponsoredContentIsNotPartofThisExperiment',
                    'enabled': True,
                    'value': {
                        'somePlacements': 'something_1a, something_2a, something_3a'
                    }
                }
            ],
            'firefoxLabsTitle': None
        },
        {
            'slug': 'treatment-a',
            'ratio': 1,
            'features': [
                {
                    'featureId': 'dapTelemetryIsNotPartOfThisExperiment',
                    'enabled': True,
                    'value': {
                        'enabled': True,
                        'somethingEnabled': True,
                        'someOtherExperimentList': [
                            {
                                'name': '1841986625495',
                            }
                        ]
                    }
                },
                {
                    'featureId': 'newtabSponsoredContentIsNotPartofThisExperiment',
                    'enabled': True,
                    'value': {
                        'somePlacements': 'something_1a, something_2a, something_3a'
                    }
                }
            ],
            'firefoxLabsTitle': None
        },
        {
            'slug': 'treatment-b',
            'ratio': 1,
            'features': [
                {
                    'featureId': 'dapTelemetryIsNotPartOfThisExperiment',
                    'enabled': True,
                    'value': {
                        'enabled': True,
                        'somethingEnabled': True,
                        'someOtherExperimentList': [
                            {
                                'name': '18419866254958234765',
                            }
                        ]
                    }                },
                {
                    'featureId': 'newtabSponsoredContentIsNotPartofThisExperiment',
                    'enabled': True,
                    'value': {
                        'somePlacements': 'something_1a, something_2a, something_3a'
                    }
                }
            ],
            'firefoxLabsTitle': None
        }
    ],
    'targeting': '(browserSettings.update.channel == "nightly") && ((experiment.slug in activeExperiments) || ((\n        \'browser.newtabpage.activity-stream.showSponsoredTopSites\'|preferenceValue\n    ) && (version|versionCompare(\'140.!\') >= 0) && (region in [\'US\'])))',
    'startDate': '2025-08-18',
    'enrollmentEndDate': None,
    'endDate': '2025-09-15',
    'proposedEnrollment': 7,
    'referenceBranch': 'control',
}

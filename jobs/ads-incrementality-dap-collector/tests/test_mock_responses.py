NIMBUS_SUCCESS = {
    'schemaVersion': '1.12.0',
    'slug': 'traffic-impact-study-5',
    'id': 'traffic-impact-study-5',
    'arguments': {},
    'application': 'firefox-desktop',
    'appName': 'firefox_desktop',
    'appId': 'firefox-desktop',
    'channel': 'nightly',
    'userFacingName': 'Traffic Impact Study 5',
    'userFacingDescription': 'We are running a basic traffic incrementality test to measure how our sponsored links on the new tab page influence visits to partner sites.  This analysis relies solely on anonymous, aggregate visit numbers - so no personal data is collected or shared at any point.',
    'isEnrollmentPaused': False,
    'isRollout': False,
    'bucketConfig': {
        'randomizationUnit': 'group_id',
        'namespace': 'firefox-desktop-dapTelemetry-newtabSponsoredContent-nightly-group_id-2',
        'start': 0,
        'count': 10000,
        'total': 10000
    },
    'featureIds': ['dapTelemetry', 'newtabSponsoredContent'],
    'probeSets': [],
    'outcomes': [{'slug': 'incrementality_sponsored_tiles', 'priority': 'primary'}],
    'branches': [
        {
            'slug': 'control',
            'ratio': 1,
            'feature': {
                'featureId': 'this-is-included-for-desktop-pre-95-support',
                'enabled': False,
                'value': {}
            },
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
            'feature': {
                'featureId': 'this-is-included-for-desktop-pre-95-support',
                'enabled': False,
                'value': {}
            },
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
            'feature': {
                'featureId': 'this-is-included-for-desktop-pre-95-support',
                'enabled': False,
                'value': {}
            },
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
    'proposedDuration': 35,
    'proposedEnrollment': 7,
    'referenceBranch': 'control',
    'featureValidationOptOut': False,
    'localizations': None,
    'locales': None,
    'publishedDate': '2025-08-18T21:38:41.303081Z',
    'isFirefoxLabsOptIn': False,
    'firefoxLabsTitle': '',
    'firefoxLabsDescription': '',
    'firefoxLabsDescriptionLinks': None,
    'firefoxLabsGroup': '',
    'requiresRestart': False
}

NIMBUS_NOT_AN_INCREMENTALITY_EXPERIMENT = {
    'schemaVersion': '1.12.0',
    'slug': 'something-experiment',
    'id': 'something-experiment',
    'arguments': {},
    'application': 'firefox-desktop',
    'appName': 'firefox_desktop',
    'appId': 'firefox-desktop',
    'channel': 'nightly',
    'userFacingName': 'Something Experiment',
    'userFacingDescription': 'We\'re trying something. No personal data is collected. ',
    'isEnrollmentPaused': False,
    'isRollout': False,
    'bucketConfig': {
        'randomizationUnit': 'group_id',
        'namespace': 'firefox-desktop-dapTelemetry-newtabSponsoredContent-nightly-group_id-2',
        'start': 0,
        'count': 10000,
        'total': 10000
    },
    'featureIds': ['dapTelemetryIsNotPartOfThisExperiment', 'newtabSponsoredContentIsNotPartOfThisExperiment'],
    'probeSets': [],
    'outcomes': [{'slug': 'incrementality_sponsored_tiles', 'priority': 'primary'}],
    'branches': [
        {
            'slug': 'control',
            'ratio': 1,
            'feature': {
                'featureId': 'this-is-included-for-desktop-pre-95-support',
                'enabled': False,
                'value': {}
            },
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
            'feature': {
                'featureId': 'this-is-included-for-desktop-pre-95-support',
                'enabled': False,
                'value': {}
            },
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
            'feature': {
                'featureId': 'this-is-included-for-desktop-pre-95-support',
                'enabled': False,
                'value': {}
            },
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
    'endDate': None,
    'proposedDuration': 35,
    'proposedEnrollment': 7,
    'referenceBranch': 'control',
    'featureValidationOptOut': False,
    'localizations': None,
    'locales': None,
    'publishedDate': '2025-08-18T21:38:41.303081Z',
    'isFirefoxLabsOptIn': False,
    'firefoxLabsTitle': '',
    'firefoxLabsDescription': '',
    'firefoxLabsDescriptionLinks': None,
    'firefoxLabsGroup': '',
    'requiresRestart': False
}

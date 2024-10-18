from dap_collector_ppa_dev.main import (
    parse_vector,
    validate_task_data,
    check_collection_dates,
    validate_json_config,
    AD_CONFIG_JSON_SCHEMA,
    TASK_CONFIG_JSON_SCHEMA
)
import datetime, pytest

def test_parse_vector():
    ret = parse_vector("54, 49, 340282366920938462946865773367900766208, 340282366920938462946865773367900766206, 1")
    assert ret == [54, 49, -1, -3, 1]

def test_validate_task_data():
    test_cases = [
        # Time precision is zero, expect an error message
        ({"time_precision_minutes": 0, "start_date": "2023-Oct-10", "end_date": "2023-Oct-11"},
            "time_precision_minutes can not be zero"),
        # Time precision doesn't divide the day evenly, expect an error
        ({"time_precision_minutes": 50, "start_date": "2023-Oct-10", "end_date": "2023-Oct-11"},
            "Task has time precision that does not evenly divide a day"),
        # Time precision divides the day evenly, no error
        ({"time_precision_minutes": 60, "start_date": "2023-Oct-10", "end_date": "2023-Oct-11"}, None),
        # Time precision longer than a day, not a whole multiple, expect error
        ({"time_precision_minutes": 2000, "start_date": "2023-Oct-10", "end_date": "2023-Oct-11"},
            "time_precision_minutes is longer than a day but is not a whole multiple of a day"),
        # Time precision is a multiple of days, no error
        ({"time_precision_minutes": 2880, "start_date": "2023-Oct-10", "end_date": "2023-Oct-12"}, None),
        # Time precision does not allow full coverage between start_date and end_date
        ({"time_precision_minutes": 2880, "start_date": "2023-Oct-10", "end_date": "2023-Oct-13"},
            "time_precision_minutes (2880) does not allow a full cocverage between 2023-10-10 00:00:00 and end_date: 2023-10-13 00:00:00 (4320.0 minutes )"),
    ]

    for taskdata, expected in test_cases:
        result = validate_task_data(taskdata)
        assert result == expected, f"Expected {expected}, but got {result}"

def test_check_collection_dates():
    test_cases = [
        {
            "start_collection_date": datetime.datetime(2024, 10, 25, tzinfo=datetime.timezone.utc),
            "end_collection_date": datetime.datetime(2024, 10, 26, tzinfo=datetime.timezone.utc),
            "task": {
                "start_date": "2024-Oct-25",
                "end_date": "2024-Oct-27",
                "time_precision_minutes": 60
            },
            "expected": None
        },
        {
            "start_collection_date": datetime.datetime(2024, 10, 23, tzinfo=datetime.timezone.utc),
            "end_collection_date": datetime.datetime(2024, 10, 24, tzinfo=datetime.timezone.utc),
            "task": {
                "start_date": "2024-Oct-24",
                "end_date": "2024-Oct-27",
                "time_precision_minutes": 60
            },
            "expected": "start_collection_date 2024-10-23 00:00:00+00:00 is before ad_start_date 2024-10-24 00:00:00+00:00"
        },
        {
            "start_collection_date": datetime.datetime(2024, 10, 27, tzinfo=datetime.timezone.utc),
            "end_collection_date": datetime.datetime(2024, 10, 28, tzinfo=datetime.timezone.utc),
            "task": {
                "start_date": "2024-Oct-24",
                "end_date": "2024-Oct-27",
                "time_precision_minutes": 60
            },
            "expected": "end_collection_date 2024-10-28 00:00:00+00:00 is after ad_end_date 2024-10-27 00:00:00+00:00"
        },
        {
            "start_collection_date": datetime.datetime(2024, 10, 26, tzinfo=datetime.timezone.utc),
            "end_collection_date": datetime.datetime(2024, 10, 27, tzinfo=datetime.timezone.utc),
            "task": {
                "start_date": "2024-Oct-24",
                "end_date": "2024-Oct-27",
                "time_precision_minutes": 2880
            },
            "expected": None
        },
        {
            "start_collection_date": datetime.datetime(2024, 10, 25, tzinfo=datetime.timezone.utc),
            "end_collection_date": datetime.datetime(2024, 10, 26, tzinfo=datetime.timezone.utc),
            "task": {
                "start_date": "2024-Oct-24",
                "end_date": "2024-Oct-27",
                "time_precision_minutes": 2880
            },
            "expected": "start_collection_date is not aligned with the time_precision_minutes of 2880."
        }
    ]

    for case in test_cases:
        result = check_collection_dates(case["start_collection_date"], case["end_collection_date"], case["task"])
        assert result == case["expected"], f"Failed: Expected {case['expected']} but got {result}"


def test_validate_json_config():
    test_cases = [
        # Valid ad config case
        ([
            {
                "taskId": "123",
                "taskIndex": 0,
                "advertiserInfo": {
                    "advertiserId": "adv01",
                    "adId": "ad01",
                    "placementId": "place01",
                    "campaignId": "camp01",
                    "extraInfo": {
                        "spend": 1000,
                        "budget": 5000
                    }
                }
            }
        ], ["taskId", "taskIndex"], AD_CONFIG_JSON_SCHEMA, None),
        
        # Invalid ad config (missing key)
        ([
            {
                "taskId": "123",
                "taskIndex": 0,
                "advertiserInfo": {
                    "advertiserId": "adv01",
                    "adId": "ad01",
                    "placementId": "place01"
                    # Missing campaignId and extraInfo
                }
            }
        ], ["taskId", "taskIndex"], AD_CONFIG_JSON_SCHEMA, "schema validation failed"),
        
        # Valid task config case
        ([
            {
                "task_id": "task_01",
                "time_precision_minutes": 60,
                "start_date": "2024-Oct-21",
                "end_date": "2024-Oct-22",
                "vdaf_args_structured": {
                    "length": 20,
                    "bits": 8
                },
                "vdaf": "sumvec",
                "hpke_config": "config_string"
            }
        ], ["task_id"], TASK_CONFIG_JSON_SCHEMA, None),
        
        # Invalid task config (missing key)
        ([
            {
                "task_id": "task_01",
                "time_precision_minutes": 60,
                "vdaf_args_structured": {
                    "length": 20,
                    "bits": 8
                }
                # Missing vdaf and hpke_config
            }
        ], ["task_id"], TASK_CONFIG_JSON_SCHEMA, "schema validation failed"),
        
        # Duplicate taskId and taskIndex in ad config
        ([
            {
                "taskId": "123",
                "taskIndex": 0,
                "advertiserInfo": {
                    "advertiserId": "adv01",
                    "adId": "ad01",
                    "placementId": "place01",
                    "campaignId": "camp01",
                    "extraInfo": {
                        "spend": 1000,
                        "budget": 5000
                    }
                }
            },
            {
                "taskId": "123",  # Duplicate taskId
                "taskIndex": 1,
                "advertiserInfo": {
                    "advertiserId": "adv02",
                    "adId": "ad02",
                    "placementId": "place02",
                    "campaignId": "camp02",
                    "extraInfo": {
                        "spend": 2000,
                        "budget": 7000
                    }
                }
            }
        ], ["taskId", "taskIndex"], AD_CONFIG_JSON_SCHEMA, "data contains duplicates for unique_keys"),
    ]

    for data, unique_keys, schema, expected_error in test_cases:
        if expected_error:
            with pytest.raises(ValueError) as excinfo:
                validate_json_config(data, unique_keys, schema)
            assert expected_error in str(excinfo.value)
        else:
            validate_json_config(data, unique_keys, schema)
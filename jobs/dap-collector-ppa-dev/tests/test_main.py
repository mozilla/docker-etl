from dap_collector_ppa_prod.main import parse_vector, find_ad_start_offset, json_array_find_duplicates
import datetime


def test_parse_vector():
    ret = parse_vector("54, 49, 340282366920938462946865773367900766208, 340282366920938462946865773367900766206, 1")
    assert ret == [54, 49, -1, -3, 1]

def test_find_ad_start_offset():
    test_cases = [
        ("2024-10-21T00:00:00+00:00", 86400 * 4),   # Monday, offset from Thursday (4 days)
        ("2024-10-22T00:00:00+00:00", 86400 * 5),   # Tuesday, offset from Thursday (5 days)
        ("2024-10-23T00:00:00+00:00", 86400 * 6),   # Wednesday, offset from Thursday (6 days)
        ("2024-10-24T00:00:00+00:00", 0),           # Thursday, no offset
        ("2024-10-25T00:00:00+00:00", 86400 * 1),   # Friday, offset from Thursday (1 day)
        ("2024-10-26T00:00:00+00:00", 86400 * 2),   # Saturday, offset from Thursday (2 days)
        ("2024-10-27T00:00:00+00:00", 86400 * 3),   # Sunday, offset from Thursday (3 days)
    ]
    for iso_time_str, expected_offset in test_cases:
        date = datetime.datetime.fromisoformat(iso_time_str)
        task = {"ad_start_date_iso": date.isoformat()}
        offset = find_ad_start_offset(task)
        assert offset == expected_offset, f"Expected {expected_offset} but got {offset} for date {iso_time_str}"

def test_json_array_find_duplicates():
    test_cases = [
        # Test case 1: No duplicates
        ([
            {"id": 1, "name": "Alice"},
            {"id": 2, "name": "Bob"},
            {"id": 3, "name": "Charlie"}
        ], ["id", "name"], {}),
        # Test case 2: Duplicate ids and names
        ([
            {"id": 1, "name": "Alice"},
            {"id": 2, "name": "Bob"},
            {"id": 1, "name": "Charlie"},
            {"id": 3, "name": "Bob"}
        ], ["id", "name"], {"id": [1], "name": ["Bob"]}),
        # Test case 3: Empty array
        ([], ["id", "name"], {}),
        # Test case 4: Missing key
        ([
            {"id": 1, "name": "Alice"},
            {"id": 2},
            {"id": 3, "name": "Charlie"}
        ], ["id", "name"], {}),
        # Test case 5: Single duplicate
        ([
            {"id": 1, "name": "Alice"},
            {"id": 1, "name": "Bob"}
        ], ["id"], {"id": [1]})
    ]
    
    for i, (json_array, keys, expected) in enumerate(test_cases):
        duplicates = json_array_find_duplicates(json_array, keys)
        assert duplicates == expected, f"Expected {expected} but got {duplicates}"
import kinto_http
import pytest

from quicksuggest2bq.main import KintoSuggestion, download_suggestions
from pytest_mock.plugin import MockerFixture
from typing import List, Dict

SAMPLE_SUGGESTION = {
    "id": 2802,
    "url": "https://www.example.com",
    "click_url": "https://example.com/click",
    "impression_url": "https://example.com/impression",
    "iab_category": "22 - Shopping",
    "icon": "4072021",
    "advertiser": "Someone",
    "title": "Someone's Website",
    "keywords": [
        "sample d",
        "sample da",
        "sample dat",
    ],
}

SAMPLE_WIKIPEDIA_SUGGESTION = {
    "id": 0,
    "url": "https://www.wikipedia.org",
    "iab_category": "5 - Education",
    "icon": "4072021",
    "advertiser": "Wikipedia",
    "title": "Main_Page",
    "keywords": [
        "wiki",
        "wikip",
        "wikipe",
    ],
}


@pytest.fixture()
def mocked_kinto_client(mocker: MockerFixture):
    session = mocker.MagicMock()

    mock_server_info = {"capabilities": {"attachments": {"base_url": "discarded"}}}

    mock_records = [
        {"type": "data", "id": 2802, "attachment": {"location": "discarded/again"}},
        {
            "type": "offline-expansion-data",
            "id": 0,
            "attachment": {"location": "discarded/again"},
        },
    ]

    class MockResponse:
        status_code = 200

        def __init__(self, attachment):
            self.attachment = attachment

        def json(self) -> List[Dict]:
            return self.attachment

    client = kinto_http.Client(session=session, bucket="mybucket")

    mocker.patch.object(client, "server_info", return_value=mock_server_info)
    mocker.patch.object(client, "get_records", return_value=mock_records)
    mocker.patch(
        "requests.Session.get",
        side_effect=[
            MockResponse([SAMPLE_SUGGESTION]),
            MockResponse([SAMPLE_WIKIPEDIA_SUGGESTION]),
        ],
    )

    yield client


@pytest.fixture()
def mocked_kinto_client_icon_only(mocker: MockerFixture):
    session = mocker.MagicMock()

    mock_server_info = {"capabilities": {"attachments": {"base_url": "discarded"}}}

    mock_records = [
        {"type": "icon", "id": 1, "attachment": {"location": "discarded/again"}},
    ]

    client = kinto_http.Client(session=session, bucket="mybucket")

    mocker.patch.object(client, "server_info", return_value=mock_server_info)
    mocker.patch.object(client, "get_records", return_value=mock_records)

    yield client


class TestMain:
    def test_suggestion_breaks_on_unknown_fields(self):
        with pytest.raises(Exception):
            KintoSuggestion(**{"does_not_exist": "i am sure!"})

    def test_suggestion_properties_are_properly_parsed(self):
        KintoSuggestion(**SAMPLE_SUGGESTION)

    def test_suggestion_download(self, mocked_kinto_client):
        suggestions = list(download_suggestions(mocked_kinto_client))
        assert len(suggestions) == 2
        assert suggestions[0] == KintoSuggestion(**SAMPLE_SUGGESTION)
        assert suggestions[1] == KintoSuggestion(**SAMPLE_WIKIPEDIA_SUGGESTION)

    def test_icon_records_not_downloaded(self, mocked_kinto_client_icon_only):
        suggestions = list(download_suggestions(mocked_kinto_client_icon_only))
        assert len(suggestions) == 0

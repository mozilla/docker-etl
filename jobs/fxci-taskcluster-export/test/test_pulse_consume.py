import pytest

from fxci_etl.pulse import consume


@pytest.fixture
def mock_pulse(mocker):
    mocker.patch.object(consume, "get_connection")
    consumer = mocker.patch.object(consume, "get_consumer").return_value
    consumer.__enter__.return_value.queues[0].queue_declare.return_value.message_count = 0
    consume.get_connection.return_value.__enter__.return_value.drain_events.side_effect = TimeoutError


def test_drain_processes_all_buffers_on_error(mocker, make_config, mock_pulse):
    failing = mocker.MagicMock()
    failing.process_buffer.side_effect = Exception("boom")
    other = mocker.MagicMock()

    with pytest.raises(Exception, match="boom"):
        consume.drain(make_config(), "task-completed", [failing, other])

    failing.process_buffer.assert_called_once()
    other.process_buffer.assert_called_once()

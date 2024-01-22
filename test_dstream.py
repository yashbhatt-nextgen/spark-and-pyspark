import pytest
from unittest.mock import patch, MagicMock
from word_count_Dstream import DStreamDemo 

@pytest.fixture
def mock_socket_text_stream():
    with patch('pyspark.streaming.StreamingContext.socketTextStream') as mock:
        yield mock

def test_process_stream(mock_socket_text_stream):
    host = 'localhost'
    port = 9000
    batch_interval = 5

    dstream_instance = DStreamDemo(host, port, batch_interval)

    mock_socket_text_stream.assert_called_once_with(host, port)

def test_start_streaming():
    host = 'localhost'
    port = 9000
    batch_interval = 5
    dstream_instance = DStreamDemo(host, port, batch_interval)

    with patch.object(dstream_instance.ssc, 'start') as mock_start, \
         patch.object(dstream_instance.ssc, 'awaitTermination') as mock_await_termination:
        dstream_instance.start_streaming()

    mock_start.assert_called_once()
    mock_await_termination.assert_called_once()

if __name__ == '__main__':
    pytest.main()

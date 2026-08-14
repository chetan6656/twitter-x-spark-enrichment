import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parents[1] / "worker"))

from zyte_worker_service import request_with_retry


class Response:
    def __init__(self, status, payload=None):
        self.status_code = status
        self._payload = payload or {}
        self.text = "error"

    def json(self):
        return self._payload


class Client:
    def __init__(self, responses):
        self.responses = iter(responses)

    def post(self, *args, **kwargs):
        return next(self.responses)


def test_worker_retries_then_succeeds():
    result = request_with_retry(Client([Response(503), Response(200, {"organicResults": []})]), "key", "q", 3)
    assert result[2] == "success"
    assert result[1] == 2


def test_worker_stops_on_credential_error():
    result = request_with_retry(Client([Response(401)]), "key", "q", 3)
    assert result[2] == "permanent_failure"
    assert result[1] == 1

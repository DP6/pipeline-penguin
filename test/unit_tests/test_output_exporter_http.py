from google.oauth2.service_account import Credentials
from pipeline_penguin.premise_output.output_exporter_http import (
    OutputExporterCloudFunctionHttp,
)
import pytest
import google.auth
import google.auth.transport.requests
from google.oauth2.service_account import Credentials
import requests


@pytest.fixture()
def _mock_google_auth(monkeypatch):
    class MockAuthorizedSession:
        def __init__(self, credentials):
            pass

        def post(self, service_url, headers, data):
            resp = requests.Response()
            resp.status_code = 200
            return resp

    def mock_default_credentials():
        return ("credentials", "my-project")

    def mock_from_service_account_file(credentials_path):
        return MockAuthorizedSession(credentials="credentials")

    monkeypatch.setattr(google.auth, "default", mock_default_credentials)
    monkeypatch.setattr(
        google.auth.transport.requests, "AuthorizedSession", MockAuthorizedSession
    )
    monkeypatch.setattr(
        Credentials, "from_service_account_file", mock_from_service_account_file
    )


class TestOutputExporterCloudFunctionHttp:
    def test_unautheticated_request(self):
        exporter = OutputExporterCloudFunctionHttp(
            url="http://www.dp6.com.br/",
        )

        returned_value = exporter.export(body={})

        assert isinstance(returned_value.get("contents"), requests.Response)
        assert returned_value.get("contents").status_code == 200
        assert returned_value.get("result") == "success"

    def test_authenticated_request_with_default_credentials(self, _mock_google_auth):
        exporter = OutputExporterCloudFunctionHttp(
            url="http://www.dp6.com.br/", credentials_path="default"
        )

        returned_value = exporter.export(body={})

        assert isinstance(returned_value.get("contents"), requests.Response)
        assert returned_value.get("contents").status_code == 200
        assert returned_value.get("result") == "success"

    def test_authenticated_request_with_provided_credentials(self, _mock_google_auth):
        exporter = OutputExporterCloudFunctionHttp(
            url="http://www.dp6.com.br/", credentials_path="/fake_service_acc_file.json"
        )

        returned_value = exporter.export(body={})

        assert isinstance(returned_value.get("contents"), requests.Response)
        assert returned_value.get("contents").status_code == 200
        assert returned_value.get("result") == "success"

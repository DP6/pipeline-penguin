import google.auth
import google.auth.transport.requests
import pytest
import pandas as pd
from pipeline_penguin.core.premise_output.output_formatter import OutputFormatter
from pipeline_penguin.premise_output.output_formatter_rsh import OutputFormatterRSH

import pandas as pd
import requests
from requests.adapters import Response


@pytest.fixture()
def _mock_premise_output():
    def mock_premise_output():
        class MockInternalClass:
            pass

        class MockPremiseOutput:
            data_premise = MockInternalClass()
            data_node = MockInternalClass()
            column = "column"
            pass_validation = True
            failed_count = 0
            failed_values = pd.DataFrame()

        return MockPremiseOutput()

    yield mock_premise_output


@pytest.fixture()
def _mock_google_auth(monkeypatch):
    def mock_default_credentials():
        return ("credentials", "my-project")

    class MockAuthorizdSession:
        def __init__(self, credentials):
            pass

        def post(self, service_url, json):
            return requests.Response()

    monkeypatch.setattr(google.auth, "default", mock_default_credentials)
    monkeypatch.setattr(
        google.auth.transport.requests, "AuthorizedSession", MockAuthorizdSession
    )


# class TestOutputFormatterRHS:
#     def test_output_formatter_log(self):
#         assert TestOutputFormatterRHS()

#     def test_export_output(self, _mock_premise_output, _mock_google_auth):
#         premise_output = _mock_premise_output()
#         formatter = OutputFormatterRSH()

#         resp = formatter.export_output(
#             premise_output,
#             "https://cloud-function-endpoint.com.br/",
#             "my-project",
#             "analytics_to_bigquery",
#             "0.2",
#             "00-00",
#             "Unit test for OutputFormatter",
#         )

#         assert isinstance(resp, Response)

import pytest
from pipeline_penguin.core.premise_output.output_formatter import OutputFormatter
from pipeline_penguin.premise_output.output_formatter_log import OutputFormatterLog


@pytest.fixture()
def _mock_premise_output():
    def mock_premise_output():
        class MockDataPremise:
            def to_serializeble_dict(self):
                return {}

        class MockPremiseOutput:
            data_premise = MockDataPremise()

        return MockPremiseOutput()

    yield mock_premise_output


class TestOutputFormatter:
    def test_output_formatter(self):
        assert OutputFormatter()


class TestOutputFormatterLog:
    def test_output_formatter_log(self):
        assert OutputFormatterLog()

    def test_export_output(self, _mock_premise_output):
        premise_output = _mock_premise_output()
        formatter = OutputFormatterLog()

        expected_result = "{}"

        assert formatter.export_output(premise_output) == expected_result

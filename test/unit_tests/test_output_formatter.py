import pytest
import pandas as pd
from pipeline_penguin.core.premise_output.output_formatter import OutputFormatter
from pipeline_penguin.premise_output.output_formatter_log import OutputFormatterLog


@pytest.fixture()
def _mock_premise_output():
    def mock_premise_output():
        class MockDataPremise:
            name = "test_premise"

        class MockPremiseOutput:
            data_premise = MockDataPremise()

            def to_serializeble_dict(self):
                return {
                    "pass_validation": True,
                    "failed_values": pd.DataFrame().to_dict(),
                    "failed_count": 0,
                    "data_premise": {"name": self.data_premise.name},
                    "data_node": {},
                }

        return MockPremiseOutput()

    yield mock_premise_output


@pytest.fixture()
def _mock_failed_premise_output():
    def mock_failed_premise_output():
        class MockDataPremise:
            name = "test_premise"

        class MockPremiseOutput:
            data_premise = MockDataPremise()

            def to_serializeble_dict(self):
                return {
                    "pass_validation": False,
                    "failed_values": pd.DataFrame([1, 2, 3]).to_dict(),
                    "failed_count": 3,
                    "data_premise": {"name": self.data_premise.name},
                    "data_node": {},
                }

        return MockPremiseOutput()

    yield mock_failed_premise_output


class TestOutputFormatter:
    def test_output_formatter(self):
        assert OutputFormatter()


class TestOutputFormatterLog:
    def test_output_formatter_log(self):
        assert OutputFormatterLog()

    def test_export_output(self, _mock_premise_output):
        premise_output = _mock_premise_output()
        formatter = OutputFormatterLog()

        expected_result = (
            "Results of test_premise validation:\n"
            "{\n"
            '    "data_node": {},\n'
            '    "data_premise": {\n'
            '        "name": "test_premise"\n'
            "    },\n"
            '    "failed_count": 0,\n'
            '    "failed_values": {},\n'
            '    "pass_validation": true\n'
            "}"
        )

        assert expected_result == formatter.export_output(premise_output)

    def test_export_output_with_failed_validation(self, _mock_failed_premise_output):
        premise_output = _mock_failed_premise_output()
        formatter = OutputFormatterLog()

        expected_result = (
            "Results of test_premise validation:\n"
            "{\n"
            '    "data_node": {},\n'
            '    "data_premise": {\n'
            '        "name": "test_premise"\n'
            "    },\n"
            '    "failed_count": 3,\n'
            '    "failed_values": {\n'
            '        "0": {\n'
            '            "0": 1,\n'
            '            "1": 2,\n'
            '            "2": 3\n'
            "        }\n"
            "    },\n"
            '    "pass_validation": false\n'
            "}"
        )

        assert formatter.export_output(premise_output) == expected_result

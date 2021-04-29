import pytest
import pandas as pd
from pipeline_penguin.core.premise_output.premise_output import PremiseOutput


@pytest.fixture()
def _mock_premise():
    def mock_premise():
        class MockDataNode:
            def __init__(self):
                self.name = "test_data_node"
                self.source = "test_source"
                self.supported_premise_types = ["test_premise_type"]

            def to_serializeble_dict(self):
                return {
                    "name": self.name,
                    "source": self.source,
                    "supported_premise_types": self.supported_premise_types,
                }

        class MockPremise:
            def __init__(self):
                self.data_node = MockDataNode()
                self.name = "test_data_premise"
                self.type = "test_premise_type"
                self.column = "test_column"
                self.query = "test_query"

            def to_serializeble_dict(self):
                return {
                    "name": self.name,
                    "type": self.type,
                    "column": self.column,
                    "query": self.query,
                }

        return MockPremise()

    yield mock_premise


@pytest.fixture()
def _mock_formatter():
    def mock_formatter():
        class MockFormatter:
            def export_output(self, premise_output):
                return premise_output

        return MockFormatter()

    yield mock_formatter


@pytest.fixture()
def _output_kwargs():
    def output_kwargs():
        return {
            "column": "test_column",
            "pass_validation": True,
            "failed_values": pd.DataFrame(),
            "failed_count": 0,
        }

    yield output_kwargs


class TestPremiseOutput:
    def test_premise_output(self, _mock_premise, _output_kwargs):
        data_premise = _mock_premise()
        output = PremiseOutput(data_premise, data_premise.data_node, **_output_kwargs())
        assert isinstance(output, PremiseOutput)

    def test_convert_to_dict(self, _mock_premise, _output_kwargs):
        data_premise = _mock_premise()
        kwargs = _output_kwargs()
        output = PremiseOutput(data_premise, data_premise.data_node, **kwargs)
        expected_result = {
            "pass_validation": kwargs["pass_validation"],
            "failed_values": kwargs["failed_values"],
            "failed_count": kwargs["failed_count"],
            "data_premise": {
                "name": data_premise.name,
                "type": data_premise.type,
                "column": data_premise.column,
                "query": data_premise.query,
            },
            "data_node": {
                "name": data_premise.data_node.name,
                "source": data_premise.data_node.source,
                "supported_premise_types": data_premise.data_node.supported_premise_types,
            },
        }

        assert output.to_serializeble_dict() == expected_result

    def test_format_output(self, _mock_premise, _mock_formatter, _output_kwargs):
        data_premise = _mock_premise()
        formatter = _mock_formatter()
        output = PremiseOutput(data_premise, data_premise.data_node, **_output_kwargs())

        assert output.format(formatter) == output

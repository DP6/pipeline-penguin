import pytest
from pipeline_penguin.core.premise_output.output_manager import OutputManager


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


@pytest.fixture()
def _mock_formatter():
    def mock_formatter():
        class MockFormatter:
            def export_output(self, premise_output):
                return "{}"

        return MockFormatter()

    yield mock_formatter


class TestOutputManager:
    def test_output_manager(self):
        output_manager = OutputManager()

        assert isinstance(output_manager, OutputManager)
        assert output_manager.outputs == {}

    def test_format_single_output(self, _mock_premise_output, _mock_formatter):
        output_manager = OutputManager()
        premise_output = _mock_premise_output()
        output_manager.outputs["test_data_node"] = {"test_data_premise": premise_output}

        formatter = _mock_formatter()
        expected_results = {"test_data_node": {"test_data_premise": "{}"}}

    ##        assert output_manager.format_outputs(formatter) == expected_results

    def test_format_many_outputs(self, _mock_premise_output, _mock_formatter):
        output_manager = OutputManager()
        premise_output_A = _mock_premise_output()
        premise_output_B = _mock_premise_output()

        # Simulating outputs
        output_manager.outputs["test_data_node_A"] = {
            "premise_output_A": premise_output_A,
            "premise_output_B": premise_output_B,
        }
        output_manager.outputs["test_data_node_B"] = {
            "premise_output_A": premise_output_A
        }

        formatter = _mock_formatter()
        expected_results = {
            "test_data_node_A": {"premise_output_A": "{}", "premise_output_B": "{}"},
            "test_data_node_B": {"premise_output_A": "{}"},
        }


##        assert output_manager.format_outputs(formatter) == expected_results

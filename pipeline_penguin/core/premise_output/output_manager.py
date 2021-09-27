r"""Core premise_output module, contains the `OutputManager` class.

OutputManager is a Singleton which stores every `PremiseOutput` returned by the validations. It can
be used to mass format all PremiseOutputs on the same OutputFormatter type.

Location: pipeline_penguin/core/premise_output/

Example usage:

```python
output_manager = OutputManager()
premise_output = _mock_premise_output()
output_manager.outputs["test_data_node"] = {"test_data_premise": premise_output}

formatter = _mock_formatter()
expected_results = {"test_data_node": {"test_data_premise": "{}"}}

output_manager.format_outputs(formatter) == expected_results
```
"""


class OutputManager:
    """An OutputManager stores "PremiseOutputs" returned by DataPremises validations.

    Attributes:
        outputs: Dictionary structure containing the PremiseOutputs
    """

    def __init__(self):
        """Initialization of the DataPremise."""
        self.outputs = {}

    def format_outputs(
        self, formatter: "pipeline_penguin.core.premise_output.OutputFormatter"
    ):
        """Method for applyting an OutputFormatter over every PremiseOutput.

        Args:
            formatter: The OutputFormatter instance to be applied on every PremiseOutput.
        Returns:
            A `dictionary` containing the formatted results.
        """
        results = {}

        for node_name, data_node in self.outputs.items():
            results[node_name] = {}
            for premise_name, premise_output in data_node.items():
                results[node_name][premise_name] = formatter.export_output(
                    premise_output
                )

        return results

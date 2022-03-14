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
        self.outputs = {}

    def export(
        self,
        formatter: "pipeline_penguin.core.premise_output.OutputFormatter",
        exporter: "pipeline_penguin.core.premise_output.OutputExporter",
        *args,
        **kwargs
    ) -> None:
        """Method for sending every validation result to a given destination

        Args:
            formatter OutputFormatter): Formatter to use on the PremiseOutputs
            exporter (OutputExporter): Exporter to use for sending the results

        Returns:
            None
        """
        
        for data_premise_name, premise_output in self.outputs.items():
            premise_output.export(formatter, exporter, *args, **kwargs)

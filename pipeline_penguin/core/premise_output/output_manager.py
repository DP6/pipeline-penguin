r"""Core premise_output module, contains the `OutputManager` class.

OutputManager is a Singleton which stores every `PremiseOutput` returned by the validations. It can
be used to mass format and export PremiseOutputs.

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
        outputs: Dictionary structure containing data for every PremiseOutput and its formatation and exportation. The structure is the following:

        | Key                                             | Value Type    | Description                                                                      |
        |-------------------------------------------------|---------------|----------------------------------------------------------------------------------|
        | [{node_name}]                                   | Dictionary    | Dictionary for every premise executed on this DataNode                           |
        | [{node_name}][{premise_name}]                   | Dictionary    | Dictionary containing details of a DataPremise on a DataNode                     |
        | [{node_name}][{premise_name}][output]           | PremiseOutput | An instance of a PremiseOutput detailing the results of a DataPremise validation |
        | [{node_name}][{premise_name}][export_status]    | Bool          | If the export was successful or not                                              |
        | [{node_name}][{premise_name}][formatted_output] | *             | The formatted output used in the exporter                                        |
    """

    def __init__(self):
        """Initialization of the DataPremise."""
        self.outputs = {}

    def export_outputs(self, formatter: "OutputFormatter"):
        """Method for applying an OutputFormatter over every PremiseOutput. Then exporting it using an OutputExporter

        Args:
            formatter: The OutputFormatter instance to be applied on every PremiseOutput.
            exporter: The OutputExporter instance to export every PremiseOutput after applying an formatation.
        Returns:
            The outputs attribute. A `dictionary` containing data for every PremiseOutput and its formatation and exportation.
        """
        results = {}

        for node_name, data_node in self.outputs.items():
            results[node_name] = {}
            for premise_name, premise_output in data_node.items():
                results[node_name][premise_name] = formatter.export_output(
                    premise_output
                )

        return results

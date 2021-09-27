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
    def __init__(self):
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
        self.outputs = {}

    def format_outputs(self, formatter: "OutputFormatter"):
        """Method for applying an OutputFormatter over every PremiseOutput.

        Args:
            formatter (OutputFormatter): The OutputFormatter to be applied on every PremiseOutput.
        Returns:
            Dict: Dictionary containing the formatted PremiseOutputs
        """

        results = {}

        for node_name, data_node in self.outputs.items():
            results[node_name] = {}
            for premise_name, premise_output in data_node.items():
                current_result = {}
                current_result["premise_output"] = premise_output
                current_result["formatted_output"] = formatter.format(premise_output)
                results[node_name][premise_name] = current_result

        return results

    def export_outputs(self, formatter: "OutputFormatter", exporter: "OutputExporter"):
        """Method for applying an OutputFormatter over every PremiseOutput. Then exporting it using an OutputExporter

        Args:
            formatter (OutputFormatter): The OutputFormatter to be applied on every PremiseOutput.
            exporter (OutputExporter): The exporter to use for sending the results.
        Returns:
            The outputs attribute. A `dictionary` containing the formatted PremiseOutputs and exports.
        """
        results = self.format_outputs(formatter=formatter)

        for node_name in results:
            for premise_name in results[node_name]:
                current_result = results[node_name][premise_name]
                formatted_output = current_result["formatted_output"]
                current_result["export_status"] = exporter.export(
                    formatted_output.get("result")
                )

        return results

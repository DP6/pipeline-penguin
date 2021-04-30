from pipeline_penguin.core.premise_output.premise_output import PremiseOutput

"""Manage all the OutputPremises from a validation"""


class OutputManager:
    """A OutputManager represents an collection of results from DataPremises validations.

    Attributes:
        outputs: Dictionary structure containing the PremiseOutputs ordered by
    """

    def __init__(self):
        """Initialization of the DataPremise."""
        self.outputs = {}

    def format_outputs(self, formatter: "OutputFormatter"):
        """Method for formating output from all PremiseOutputs on outputs"""
        results = {}

        for node_name, data_node in self.outputs.items():
            results[node_name] = {}
            for premise_name, premise_output in data_node.items():
                results[node_name][premise_name] = formatter.export_output(
                    premise_output
                )

        return results

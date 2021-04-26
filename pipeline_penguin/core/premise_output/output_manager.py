"""Manage all the OutputPremises from a validation"""


class OutputManager:
    """A OutputManager represents an collection of results from DataPremises validations.

    Args:
        outputs: Dictionary containing DataNodes, DataPremises and their PremiseOutputs
    """

    def __init__(self):
        """Initialization of the DataPremise."""
        self.outputs = {}

    def format_outputs(self, formatter: "OutputFormatter"):
        """Method for formating output from all PremiseOutputs on outputs"""
        pass

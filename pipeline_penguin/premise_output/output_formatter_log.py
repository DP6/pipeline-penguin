"""Print the result of a DataPremise Validation"""
from pipeline_penguin.core.premise_output.output_formatter import OutputFormatter


class OutputFormatterLog(OutputFormatter):
    """A OutputFormatterLog represents a way to format the PremiseOutput class as an log"""

    def export_output(self, premise_output: "PremiseOutput"):
        """Generate an formated output from a PremiseOutput class"""
        pass

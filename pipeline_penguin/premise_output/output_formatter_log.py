"""Print the result of a DataPremise Validation"""
from pipeline_penguin.core.premise_output.output_formatter import OutputFormatter
import json


class OutputFormatterLog(OutputFormatter):
    """A OutputFormatterLog represents a way to format the PremiseOutput class as readable log"""

    def export_output(self, premise_output: "PremiseOutput") -> str:
        """Construct a human-readable message containing the results of a premise validation.

        Args:
            premise_output: PremiseOutput object to be formatted
        Returns:
            str: Human-readable message with PremiseOutput's data
        """
        data_premise = premise_output.data_premise
        output_data = premise_output.to_serializeble_dict()

        json_data = json.dumps(output_data, indent=4, sort_keys=True)

        msg = f"""Results of {data_premise.name} validation:\n{json_data}"""
        return msg

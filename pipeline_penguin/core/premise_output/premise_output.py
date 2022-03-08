r"""Core premise_output module, contains the `PremiseOutput` class.

PremiseOutput is a data strucutre for abstracting the validation results of a single DataPremise
execution. It is designed to be used by different `OutputFormatters` instances for compiling the
results in a specific format.

Location: pipeline_penguin/core/premise_output/

Example usage:

```python
class DataPremiseSQLCheckNull(DataPremiseSQL):

    # ... DataPremise initialization code ...

    def validate(self):
        # ... validation execution ...

        # Returning the results of a validation
        return PremiseOutput(
            self, self.data_node, self.colum, passed, failed_count, data_frame
        )

```
"""
import pandas as pd
from typing import Dict


class PremiseOutput:
    """A PremiseOutput represents the results of a DataPremise Validation.

    Args:
        data_premise: The DataPremise that generated this output.
        data_node: The DataNode related to the DataPremise ran on.
        column: Name of the validated column.
        pass_validation: Indicated whether the data passed the validation or not.
        failed_count: Number of incorrect results returned by the DataPremise.
        failed_values: A pandas dataframe with the incorrect results returned by the DataPremise
    Attributes
        data_premise: The DataPremise that generated this output.
        data_node: The DataNode related to the DataPremise ran on.
        column: Name of the validated column.
        pass_validation: Indicated whether the data passed the validation or not.
        failed_count: Number of incorrect results returned by the DataPremise.
        failed_values: A pandas dataframe with the incorrect results returned by the DataPremise
    """

    def __init__(
        self,
        data_premise: "pipeline_penguin.core.data_premise.DataPremise",
        data_node: "pipeline_penguin.core.data_node.DataNode",
        column: str,
        pass_validation: bool,
        failed_count: int,
        failed_values: pd.DataFrame,
    ):
        self.data_premise = data_premise
        self.data_node = data_node
        self.column = column
        self.pass_validation = pass_validation
        self.failed_count = failed_count
        self.failed_values = failed_values

    def to_serializeble_dict(self) -> Dict:
        """Returns a dictionary representation of the current PremiseOutput using
        python's built-in data types.

        Returns:
            A `dictionary` object containing the PremiseOutput representation.
        """
        results = {
            "pass_validation": self.pass_validation,
            "failed_values": self.failed_values.to_dict(),
            "failed_count": self.failed_count,
        }
        results.update({"data_premise": self.data_premise.to_serializeble_dict()})
        results.update({"data_node": self.data_node.to_serializeble_dict()})

        return results

    def export(
        self,
        formatter: "pipeline_penguin.core.premise_output.OutputFormatter",
        exporter: "pipeline_penguin.core.premise_output.OutputExporter",
        *args,
        **kwargs
    ) -> None:
        """Applies a given "OutputFormatter" on the PremiseOutput and exports
        the resulting data using the provided "OutputExporter".
        
        Args:
            formatter (OutputFormatter): Formatter to use on the PremiseOutput
            exporter (OutputExporter): Exporter to use for sending the results

        Returns:
            None
        """
        formatterResult = ""

        formatterResult = formatter.format(self)
        exporter.export_output(formatterResult, *args, **kwargs)

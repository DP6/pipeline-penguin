"""Premise for checking SQL null values."""

from typing import Union
from pipeline_penguin.core.data_premise.sql import DataPremiseSQL
from pipeline_penguin.core.premise_output.premise_output import PremiseOutput
from pipeline_penguin.exceptions import WrongTypeReference


class DataPremiseCheckInArray(DataPremiseSQL):
    """This DataPremise is responsible for validating if a given column does not have null values.

    Args:
        name: Name of the premise.
        column: Column to be validated.
    Attributes:
        query: SQL query to be executed for premise validation.
        type: Constant indicating the type of the premise (SQL).
    """

    def __init__(
        self,
        name: str,
        data_node: "DataNodeBigQuery",
        column: str,
        array: Union[str, list, float, bool],  # TODO Add datetime support
    ):
        """Initialize the DataPremise after building the validation query."""
        if type(array) == "list":
            array = str(array)

        self.query_template = "SELECT count(*) total FROM `{project}.{dataset}.{table}` WHERE {column} IN UNNEST({array})"
        self.array = array
        super().__init__(name, data_node, column)

    def query_args(self):
        return {
            "project": self.data_node.project_id,
            "dataset": self.data_node.dataset_id,
            "table": self.data_node.table_id,
            "column": self.column,
            "array": self.array,
        }

    def validate(self) -> PremiseOutput:
        """Run the validation function.

        Returns:
            PremiseOutput: Wrapper object for the validation results
        """

        query = self.query_template.format(**self.query_args())
        connector = self.data_node.get_connector(self.type)
        data_frame = connector.run(query)

        failed_count = data_frame["total"][0]
        passed = failed_count == 0

        output = PremiseOutput(
            self, self.data_node, self.column, passed, failed_count, data_frame
        )
        return output

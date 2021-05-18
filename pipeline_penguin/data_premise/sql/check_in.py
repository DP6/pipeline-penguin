"""Premise for checking the IN logical operator."""

from typing import Union
from pipeline_penguin.core.data_premise.sql import DataPremiseSQL
from pipeline_penguin.core.premise_output.premise_output import PremiseOutput
from pipeline_penguin.exceptions import WrongTypeReference


class DataPremiseSQLCheckInArray(DataPremiseSQL):
    """This DataPremise is responsible for validating if a the values of given column are
    contained by the provided array.
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

        self.query_template = "SELECT * result FROM `{project}.{dataset}.{table}` WHERE {column} IN UNNEST({array})"
        self.array = array
        super().__init__(name, data_node, column)

    def query_args(self):
        """Arguments for building the Premise's validation query."""
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

        failed_count = len(data_frame["result"])
        passed = failed_count == 0

        output = PremiseOutput(
            self, self.data_node, self.column, passed, failed_count, data_frame
        )
        return output

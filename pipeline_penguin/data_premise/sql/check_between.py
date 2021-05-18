"""Premise for checking a bound of values."""

from pipeline_penguin.core.data_premise.sql import DataPremiseSQL
from pipeline_penguin.core.premise_output.premise_output import PremiseOutput


class DataPremiseSQLCheckValuesAreBetween(DataPremiseSQL):
    """This DataPremise is responsible for validating if a given column is between a given range of values."""

    def __init__(
        self,
        name: str,
        data_node: "DataNodeBigQuery",
        column: str,
        lower_bound: str,
        upper_bound: str,
    ):
        """Initialize the DataPremise after building the validation query."""

        self.query_template = "SELECT * result FROM `{project}.{dataset}.{table}` WHERE  {column} BETWEEN {lower_bound} AND {upper_bound}"
        self.lower_bound = lower_bound
        self.upper_bound = upper_bound
        super().__init__(name, data_node, column)

    def query_args(self):
        """Arguments for building the Premise's validation query."""
        return {
            "project": self.data_node.project_id,
            "dataset": self.data_node.dataset_id,
            "table": self.data_node.table_id,
            "column": self.column,
            "lower_bound": self.lower_bound,
            "upper_bound": self.upper_bound,
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

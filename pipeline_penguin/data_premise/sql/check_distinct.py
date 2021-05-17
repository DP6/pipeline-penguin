"""Premise for checking distinct values."""

from pipeline_penguin.core.data_premise.sql import DataPremiseSQL
from pipeline_penguin.core.premise_output.premise_output import PremiseOutput


class DataPremiseSQLCheckDistinct(DataPremiseSQL):
    """This DataPremise is responsible for validating if a given column only has distinct values."""

    def __init__(self, name: str, data_node: "DataNodeBigQuery", column: str):
        """Initialize the DataPremise after building the validation query."""

        super().__init__(name, data_node, column)
        self.query_template = "SELECT count(DISTINCT {column}) distinct, count({column}) total as total FROM `{project}.{dataset}.{table}`"

    def query_args(self):
        """Arguments for building the Premise's validation query."""
        return {
            "project": self.data_node.project_id,
            "dataset": self.data_node.dataset_id,
            "table": self.data_node.table_id,
            "column": self.column,
        }

    def validate(self) -> PremiseOutput:
        """Run the validation function.

        Returns:
            PremiseOutput: Wrapper object for the validation results
        """

        query = self.query_template.format(**self.query_args())
        connector = self.data_node.get_connector(self.type)
        data_frame = connector.run(query)

        passed = data_frame["result"][0] == data_frame["total"][0]
        failed_count = data_frame["total"][0] - data_frame["result"][0]

        output = PremiseOutput(
            self, self.data_node, self.column, passed, failed_count, data_frame
        )
        return output

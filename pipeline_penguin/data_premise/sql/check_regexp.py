"""Premise for checking regexp operations."""

from pipeline_penguin.core.data_premise.sql import DataPremiseSQL
from pipeline_penguin.core.premise_output.premise_output import PremiseOutput
from pipeline_penguin.exceptions import WrongTypeReference


class DataPremiseSQLCheckRegexpContains(DataPremiseSQL):
    """This DataPremise is responsible for validating if the values of a given column matches a regexp pattern."""

    def __init__(
        self,
        name: str,
        data_node: "DataNodeBigQuery",
        column: str,
        pattern: str,
    ):
        """Initialize the DataPremise after building the validation query."""

        # TODO: Deal with negativated regexp_contains

        self.query_template = "SELECT COUNT(*) total FROM `{project}.{dataset}.{table}` WHERE REGEXP_CONTAINS({column}, {pattern})"
        self.pattern = pattern
        super().__init__(name, data_node, column)

    def query_args(self):
        """Arguments for building the Premise's validation query."""
        return {
            "project": self.data_node.project_id,
            "dataset": self.data_node.dataset_id,
            "table": self.data_node.table_id,
            "column": self.column,
            "pattern": self.pattern,
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

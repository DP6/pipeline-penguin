"""Premise for checking a logical comparison."""

from pipeline_penguin.core.data_premise.sql import DataPremiseSQL
from pipeline_penguin.core.premise_output.premise_output import PremiseOutput
from pipeline_penguin.exceptions import WrongTypeReference


class DataPremiseSQLCheckLogicalComparisonWithValue(DataPremiseSQL):
    """This DataPremise is responsible for validating if a logical expression involving the given
    column is True.
    """

    def __init__(
        self,
        name: str,
        data_node: "DataNodeBigQuery",
        column: str,
        operator: str,
        value: str,
    ):
        """Initialize the DataPremise after building the validation query."""
        supported_operators = [
            "<",
            "<=",
            "=",
            "=>",
            "!=",
            "<>",
        ]
        if operator not in supported_operators:
            raise WrongTypeReference(
                f"Operator not supported, supported operators: {supported_operators}"
            )

        self.query_template = "SELECT * result FROM `{project}.{dataset}.{table}` WHERE {column} {operator} {value}"
        self.operator = operator
        self.value = value
        super().__init__(name, data_node, column)

    def query_args(self):
        """Arguments for building the Premise's validation query."""
        return {
            "project": self.data_node.project_id,
            "dataset": self.data_node.dataset_id,
            "table": self.data_node.table_id,
            "column": self.column,
            "operator": self.operator,
            "value": self.value,
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

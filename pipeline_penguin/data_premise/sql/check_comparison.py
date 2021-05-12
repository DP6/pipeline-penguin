"""Premise for checking SQL null values."""

from pipeline_penguin.core.data_premise.sql import DataPremiseSQL
from pipeline_penguin.core.premise_output.premise_output import PremiseOutput
from pipeline_penguin.exceptions import WrongTypeReference


class DataPremiseCheckLogicalComparisonWithValue(DataPremiseSQL):
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

        self.query_template = "SELECT count({column} {operator} {value}) result, count({column}) total FROM `{project}.{dataset}.{table}`"
        self.operator = operator
        self.value = value
        super().__init__(name, data_node, column)

    def query_args(self):
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

        passed = data_frame["result"][0] == data_frame["total"][0]
        failed_count = data_frame["total"][0] - data_frame["result"][0]

        output = PremiseOutput(
            self, self.data_node, self.column, passed, failed_count, data_frame
        )
        return output

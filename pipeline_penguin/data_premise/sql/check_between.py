"""Premise for checking SQL null values."""

from pipeline_penguin.core.data_premise.sql import DataPremiseSQL
from pipeline_penguin.core.premise_output.premise_output import PremiseOutput
from pipeline_penguin.exceptions import WrongTypeReference


class DataPremiseCheckValuesAreBetween(DataPremiseSQL):
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
        lower_bound: str,
        upper_bound: str,
    ):
        """Initialize the DataPremise after building the validation query."""

        self.query_template = "SELECT COUNT({column} BETWEEN {lower_bound} AND {upper_bound}) result, count({column}) total FROM `{project}.{dataset}.{table}`"
        self.lower_bound = lower_bound
        self.upper_bound = upper_bound
        super().__init__(name, data_node, column)

    def query_args(self):
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

        passed = data_frame["result"][0] == data_frame["total"][0]
        failed_count = data_frame["total"][0] - data_frame["result"][0]

        output = PremiseOutput(
            self, self.data_node, self.column, passed, failed_count, data_frame
        )
        return output

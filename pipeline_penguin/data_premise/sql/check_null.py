"""Premise for checking SQL null values."""

from pipeline_penguin.core.data_premise.sql import DataPremiseSQL
from pipeline_penguin.core.premise_output.premise_output import PremiseOutput


class DataPremiseSQLCheckNull(DataPremiseSQL):
    """This DataPremise is responsible for validating if a given column does not have null values.

    Args:
        name: Name of the premise.
        column: Column to be validated.
    Attributes:
        query: SQL query to be executed for premise validation.
        type: Constant indicating the type of the premise (SQL).
    """

    def __init__(self, name: str, data_node: "DataNodeBigQuery", column: str):
        """Initialize the DataPremise after building the validation query."""

        query_template = (
            "SELECT count(*) FROM `{project}.{dataset}.{table}` WHERE {column} is null"
        )
        query_args = {
            "project": data_node.project_id,
            "dataset": data_node.dataset_id,
            "table": data_node.table_id,
            "column": column,
        }

        super().__init__(name, data_node, query_template.format(**query_args))
        self.column = column

    def validate(self) -> PremiseOutput:
        """Run the validation function.

        Returns:
            bool: True if no null values are found in the given column, False otherwise.
        """
        connector = self.data_node.get_connector(self.type)
        data_frame = connector.run(self.query)
        failed_count = len(data_frame)
        passed = failed_count == 0

        output = PremiseOutput(
            self, self.data_node, self.column, passed, failed_count, data_frame
        )
        return output

    def to_serializeble_dict(self) -> dict:
        return {
            "name": self.name,
            "type": self.type,
            "column": self.column,
            "query": self.query,
        }
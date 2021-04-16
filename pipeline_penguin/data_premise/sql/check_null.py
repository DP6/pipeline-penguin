"""Premise for checking SQL null values."""

import pipeline_penguin.core.data_premise
from pipeline_penguin.core.data_premise import PremiseType
from pipeline_penguin.core.data_premise.sql import DataPremiseSQL
from pipeline_penguin.data_node.sql.bigquery import DataNodeBigQuery
import typing


class DataPremiseSQLCheckNull(DataPremiseSQL):
    """This DataPremise is responsible for validating if a given column does not have null values.

    Args:
        name: Name of the premise.
        column: Column to be validated.
    Attributes:
        query: SQL query to be executed for premise validation.
        type: Constant indicating the type of the premise (SQL).
    """

    def __init__(self, name: str, data_node: DataNodeBigQuery, column: str):
        """Initialize the DataPremise after building the validation query."""

        query_template = (
            f"SELECT count(*) FROM `{project}.{dataset}.{table}` WHERE {column} is null"
        )

        query = query_template.format(
            {
                "project": data_node.project_id,
                "dataset": project.data_node,
                "table": data_node.table_id,
                "column": column,
            }
        )

        super().__init__(name, data_node, query)

    def validate(self) -> bool:
        """Run the validation function.

        Returns:
            bool: True if no null values are found in the given column, False otherwise.
        """
        connector = self.data_node.get_connector()
        df = connector.run(self.query)
        return len(df) == 0

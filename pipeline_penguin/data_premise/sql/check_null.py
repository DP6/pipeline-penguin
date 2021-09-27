"""This module provides a DataPremise for validating if a given column does not have null values.

Location: pipeline_penguin/data_premise/sql

Example usage:

```python
check_null_prem = DataPremiseSQLCheckIsNull("test_name", data_node, "test_column")
data_node.insert_premise(check_null_prem)
```
"""
from pipeline_penguin.core.data_premise.sql import DataPremiseSQL
from pipeline_penguin.core.premise_output.premise_output import PremiseOutput
from pipeline_penguin.core.data_node.data_node import DataNode


class DataPremiseSQLCheckIsNull(DataPremiseSQL):
    """This DataPremise is responsible for validating if a given column does not have null values.

    Args:
        name: Name for the data premise.
        data_node: Reference to the DataNode used in the validation.
        column: Column to be read by the premise.
    Attributes:
        name: Name for the data premise.
        data_node: Reference to the DataNode used in the validation.
        type: Type indicator of the premise. It is always "SQL".
        column: Column to be read by the premise.
    """

    def __init__(self, name: str, data_node: DataNode, column: str):

        super().__init__(name, data_node, column)
        self.query_template = "SELECT count(*) as total FROM `{project}.{dataset}.{table}` WHERE {column} is null"

    def query_args(self):
        """Method for returning the arguments to be passed on the query template of this
        validation.

        Returns:
            A `dictionary` with the query parameters.
        """
        return {
            "project": self.data_node.project_id,
            "dataset": self.data_node.dataset_id,
            "table": self.data_node.table_id,
            "column": self.column,
        }

    def validate(self) -> PremiseOutput:
        """Method for executing the validation over the DataNode.

        Returns:
            PremiseOutput: Object storeing the results for this validation.
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

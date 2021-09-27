"""This module provides DataPremise for validating if all values of a column are distinct.

Location: pipeline_penguin/data_premise/sql

Example usage:

```python
check_distinct_prem = DataPremiseSQLCheckDistinct(
    "test_name", data_node, "test_column"
)
data_node.insert_premise(check_distinct_prem)
```
"""
from pipeline_penguin.core.data_premise.sql import DataPremiseSQL
from pipeline_penguin.core.premise_output.premise_output import PremiseOutput
from pipeline_penguin.core.data_node.data_node import DataNode


class DataPremiseSQLCheckDistinct(DataPremiseSQL):
    """This DataPremise is responsible for validating if all values of a column are distinct.

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
        self.query_template = "SELECT count(DISTINCT {column}) distinct, count({column}) total as total FROM `{project}.{dataset}.{table}`"

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

        passed = data_frame["result"][0] == data_frame["total"][0]
        failed_count = data_frame["total"][0] - data_frame["result"][0]

        output = PremiseOutput(
            self, self.data_node, self.column, passed, failed_count, data_frame
        )
        return output

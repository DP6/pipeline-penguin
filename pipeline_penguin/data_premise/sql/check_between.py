"""This module provides a DataPremise for validating if the values of a given column are within
a range of values.

Location: pipeline_penguin/data_premise/sql

Example usage:

```python
check_between_prem = DataPremiseSQLCheckValuesAreBetween(
    "test_name", data_node, "test_column", 100, 200
)
data_node.insert_premise(check_between_prem)
```
"""


from pipeline_penguin.core.data_premise.sql import DataPremiseSQL
from pipeline_penguin.core.premise_output.premise_output import PremiseOutput
from pipeline_penguin.core.data_node.data_node import DataNode


class DataPremiseSQLCheckValuesAreBetween(DataPremiseSQL):
    """This DataPremise is responsible for validating if a arithmetic operation involving the given
    column and an given term returns an expected restult (i.e. validate if column + 20 = 40).

    Args:
        name: Name for the data premise.
        data_node: Reference to the DataNode used in the validation.
        column: Column to be read by the premise.
        lower_bound: Minimum allowed value for the column
        upper_bound: Maximum allowed value for the column
    Attributes:
        name: Name for the data premise.
        data_node: Reference to the DataNode used in the validation.
        type: Type indicator of the premise. It is always "SQL".
        column: Column to be read by the premise.
        lower_bound: Minimum allowed value for the column
        upper_bound: Maximum allowed value for the column
    """

    def __init__(
        self,
        name: str,
        data_node: DataNode,
        column: str,
        lower_bound: str,
        upper_bound: str,
    ):

        self.query_template = "SELECT {column} as result FROM `{project}.{dataset}.{table}` WHERE  {column} BETWEEN {lower_bound} AND {upper_bound}"
        self.lower_bound = lower_bound
        self.upper_bound = upper_bound
        super().__init__(name, data_node, column)

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
            "lower_bound": self.lower_bound,
            "upper_bound": self.upper_bound,
        }

    def validate(self) -> PremiseOutput:
        """Method for executing the validation over the DataNode.

        Returns:
            PremiseOutput: Object storeing the results for this validation.
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

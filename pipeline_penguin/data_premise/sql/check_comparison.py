"""This module provides DataPremise for validating if a logical operation between a column and a
provided value is true.

Location: pipeline_penguin/data_premise/sql

Example usage:

```python
check_logical_prem = DataPremiseSQLCheckLogicalComparisonWithValue(
    "test_name", data_node, "test_column", "<", 100
)
data_node.insert_premise(check_logical_prem)
```
"""

from pipeline_penguin.core.data_premise.sql import DataPremiseSQL
from pipeline_penguin.core.premise_output.premise_output import PremiseOutput
from pipeline_penguin.exceptions import WrongTypeReference
from pipeline_penguin.core.data_node.data_node import DataNode


class DataPremiseSQLCheckLogicalComparisonWithValue(DataPremiseSQL):
    """This DataPremise is responsible for validating if a logical operation between a column and a
    provided value is true. (i.e. validate if column >= 20).

    Args:
        name: Name for the data premise.
        data_node: Reference to the DataNode used in the validation.
        column: Column to be read by the premise.
        operator: The logical operator (<,<=,=,=>,!=,<>).
        value: Value for the second term of the operation.
    Attributes:
        name: Name for the data premise.
        data_node: Reference to the DataNode used in the validation.
        type: Type indicator of the premise. It is always "SQL".
        column: Column to be read by the premise.
        operator: The logical operator (<,<=,=,=>,!=,<>).
        value: Value for the second term of the operation.
    Raises:
        WrongTypeReference: If the "operator" argument is not a supported character ["<","<=","=",
        "=>","!=","<>"]
    """

    def __init__(
        self,
        name: str,
        data_node: DataNode,
        column: str,
        operator: str,
        value: str,
    ):
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

        self.query_template = "SELECT {column} result FROM `{project}.{dataset}.{table}` WHERE {column} {operator} {value}"
        self.operator = operator
        self.value = value
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
            "operator": self.operator,
            "value": self.value,
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

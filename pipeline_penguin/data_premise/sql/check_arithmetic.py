"""This module provides a generalized arithmetic DataPremise, used to check if a arithmetic
operation between a column and a second term is equal to an given result.

It currently supports addition, subtraction, multiplication and division of [numeric data types](
https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types#numeric_types).

Location: pipeline_penguin/data_premise/sql

Example usage:

```python
check_arithmetic_prem = DataPremiseSQLCheckArithmeticOperationEqualsResult(
    "test_name", data_node, "test_column", "+", 20, 20
)
data_node.insert_premise(check_arithmetic_prem)
```
"""

from pipeline_penguin.core.data_premise.sql import DataPremiseSQL
from pipeline_penguin.core.premise_output.premise_output import PremiseOutput
from pipeline_penguin.exceptions import WrongTypeReference
from typing import Union
from pipeline_penguin.core.data_node.data_node import DataNode


class DataPremiseSQLCheckArithmeticOperationEqualsResult(DataPremiseSQL):
    """This DataPremise is responsible for validating if a arithmetic operation involving the given
    column and an given term returns an expected restult (i.e. validate if column + 20 = 40).

    Args:
        name: Name for the data premise.
        data_node: Reference to the DataNode used in the validation.
        column: Column to be read by the premise.
        operator: The arithmetic operator (+, -, *, /).
        second_term: Numeric value for the second term of the operation.
        expected_result: Expected numeric value for the result of the operation.
    Attributes:
        name: Name for the data premise.
        data_node: Reference to the DataNode used in the validation.
        type: Type indicator of the premise. It is always "SQL".
        column: Column to be read by the premise.
        operator: The arithmetic operator (+, -, *, /).
        second_term: Numeric value for the second term of the operation.
        expected_result: Expected numeric value for the result of the operation.
    Raises:
        WrongTypeReference: If the "operator" argument is not a supported character ["+", "-", "*",
        "/"]
    """

    def __init__(
        self,
        name: str,
        data_node: DataNode,
        column: str,
        operator: str,
        second_term: Union[int, float],
        expected_result: Union[int, float],
    ):
        super().__init__(name, data_node, column)
        supported_operators = ["+", "-", "*", "/"]
        if operator not in supported_operators:
            raise WrongTypeReference(
                f"Operator not supported, supported operators: {supported_operators}"
            )
        self.query_template = "SELECT {column} as result FROM `{project}.{dataset}.{table}` WHERE {column} {operator} {second_term} = {expected_result}"
        self.operator = operator
        self.second_term = second_term
        self.expected_result = expected_result

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
            "second_term": self.second_term,
            "expected_result": self.expected_result,
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

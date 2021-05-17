"""Premise for checking arithmetic operations."""

from pipeline_penguin.core.data_premise.sql import DataPremiseSQL
from pipeline_penguin.core.premise_output.premise_output import PremiseOutput
from pipeline_penguin.exceptions import WrongTypeReference
from typing import Union


class DataPremiseSQLCheckArithmeticOperationEqualsResult(DataPremiseSQL):
    """This DataPremise is responsible for validating if a arithmetic operation involving the given
    column returns an expected restult.
    """

    def __init__(
        self,
        name: str,
        data_node: "DataNodeBigQuery",
        column: str,
        operator: str,
        second_term: Union[str, int, float],
        expected_result: Union[str, int, float],
    ):
        """Initialize the DataPremise after building the validation query."""

        super().__init__(name, data_node, column)
        supported_operators = ["+", "-", "*", "/"]
        if operator not in supported_operators:
            raise WrongTypeReference(
                f"Operator not supported, supported operators: {supported_operators}"
            )
        self.query_template = "SELECT COUNT(*) as total FROM `{project}.{dataset}.{table}` WHERE {column} {operator} {second_term} = {expected_result}"
        self.operator = operator
        self.second_term = second_term
        self.expected_result = expected_result

    def query_args(self):
        """Arguments for building the Premise's validation query."""
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
        """Run the validation function.

        Returns:
            PremiseOutput: Wrapper object for the validation results
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

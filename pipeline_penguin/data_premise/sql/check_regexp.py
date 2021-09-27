"""This module provides DataPremise for validating if the values of a column matches a given regexp
pattern.

Location: pipeline_penguin/data_premise/sql

Example usage:

```python
check_regexp_prem = DataPremiseSQLCheckRegexpContains(
    "test_name", data_node, "test_column", "^regexp (test){3}$"
)
data_node.insert_premise(check_regexp_prem)
```
"""

from pipeline_penguin.core.data_premise.sql import DataPremiseSQL
from pipeline_penguin.core.premise_output.premise_output import PremiseOutput
from pipeline_penguin.exceptions import WrongTypeReference
from pipeline_penguin.core.data_node.data_node import DataNode


class DataPremiseSQLCheckRegexpContains(DataPremiseSQL):
    """This DataPremise is responsible for validating if the values of a column matches a given
    regexp pattern.

    Args:
        name: Name for the data premise.
        data_node: Reference to the DataNode used in the validation.
        column: Column to be read by the premise.
        pattern: String with regex pattern to be matched against the column. Supports golang regexp.
    Attributes:
        name: Name for the data premise.
        data_node: Reference to the DataNode used in the validation.
        type: Type indicator of the premise. It is always "SQL".
        column: Column to be read by the premise.
        pattern: String with regex pattern to be matched against the column. Supports golang regexp.
    """

    def __init__(
        self,
        name: str,
        data_node: DataNode,
        column: str,
        pattern: str,
    ):

        self.query_template = 'SELECT {column} result FROM `{project}.{dataset}.{table}` WHERE REGEXP_CONTAINS({column}, r"{pattern}")'
        self.pattern = pattern
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
            "pattern": self.pattern,
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

"""Core data_premise module, contains the abstract `DataPremiseSQL` class.

DataPremise objects represent a single validation to be executed on a given `DataNode`.

This modules provides the `DataPremiseSQL` abstract constructor. Designed to be inherited by other
classes for building more specific SQL-based validations, it should not be instantiated directly.

Location: pipeline_penguin/core/data_node/

Example usage:

```python

class DataPremiseSQLCheckIsNull(DataPremiseSQL):
    def __init__(self):
        # ...
        # Code for initializing the DataPremise
        # ...
        super().__init__()

    def validate(self):
        # ...
        # Code for validating the DataPremise
        # ...
        return output
```
"""
from pipeline_penguin.core.data_node.data_node import DataNode
from . import DataPremise, PremiseType


class DataPremiseSQL(DataPremise):
    """Abstract parent constructor for building SQL-based DataPremise classes.

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

    type = PremiseType.SQL

    def __init__(
        self,
        name: str,
        data_node: "pipeline_penguin.core.data_node.DataNode",
        column: str,
    ):
        super().__init__(name, data_node)
        self.column = column

    def query_args(self):
        """Returns the arguments to be used while building the SQL query on premise execution"""
        return {}

    def to_serializeble_dict(self) -> dict:
        """Method for constructing a dictionary representation of the current DataPremise using
        only built-in data types.

        Returns:
            A `dictionary` object containing the DataPremise representation.
        """
        as_dict = {
            "name": self.name,
            "type": self.type,
            "column": self.column,
            "query_args": self.query_args(),
        }
        if hasattr(self, "query_template"):
            as_dict["query"] = getattr(self, "query_template")
        return as_dict

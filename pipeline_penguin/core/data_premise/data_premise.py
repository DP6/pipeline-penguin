"""Core data_premise module, contains the abstract `DataPremise` class.

DataPremise objects represent a single validation to be executed on a given `DataNode`.

This modules provides the `DataPremise` abstract constructor. Designed to be inherited by other
classes for building more specific validations, it should not be instantiated directly.

Location: pipeline_penguin/core/data_node/

Example usage:

```python
from pipeline_penguin.core.data_premise import DataPremise

class DataPremiseSQL(DataPremise):
    def __init__(self):
        # ...
        # Code for initializing the DataPremise
        # ...
        super().__init__()

    def to_serializeble_dict(self):
        # ...
        # Code for summarizing the DataPremise as a dictionary
        # ...
        return {}
```
"""
from typing import Dict
from pipeline_penguin.core.premise_output.premise_output import PremiseOutput


class DataPremise:
    """Abstract parent constructor for building other DataPremise classes.

    DataPremise objects represent a single validation to be executed on a given `DataNode`.

    Args:
        name: Name for the data premise.
        data_node: Reference to the DataNode used in the validation.
    Attributes:
        name: Name for the data premise.
        data_node: Reference to the DataNode used in the validation.
    """

    def __init__(
        self, name: str, data_node: "pipeline_penguin.core.data_node.DataNode"
    ):
        self.name = name
        self.data_node = data_node

    def validate(self) -> PremiseOutput:
        """Abstract method for executing the validation test."""

        pass

    def to_serializeble_dict(self) -> Dict:
        """Method for constructing a dictionary representation of the current DataPremise using
        only built-in data types.

        Returns:
            A `dictionary` object containing the DataPremise representation.
        """
        return {}

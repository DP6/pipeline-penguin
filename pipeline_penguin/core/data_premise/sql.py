"""This module provides the abstract DataPremiseSQL constructor."""
from . import DataPremise, PremiseType


class DataPremiseSQL(DataPremise):
    """Constructor for the DataPremiseSQL.

    Args:
        name: Name for the data premise.
        data_node:
        column: Column to be read by the premise.
        query: SQL query to be used on the validation.
    Attributes:
        type: Type indicator of the premise. It is always "SQL".
    """

    type = PremiseType.SQL

    def __init__(self, name: str, data_node: "DataNode", column: str):
        """Initialize the DataPremiseSQL object."""
        super().__init__(name, data_node)
        self.column = column

    def query_args(self):
        pass

    def to_serializeble_dict(self) -> dict:
        """Returns a dictionary representation of the current DataPremise using
        only built-in data types.

        Returns:
            dict -> Dicionary containing attributes of the DataPremise.
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

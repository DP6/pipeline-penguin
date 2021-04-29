"""This module provides the abstract DataPremiseSQL constructor."""
from . import DataPremise, PremiseType


class DataPremiseSQL(DataPremise):
    """Constructor for the DataPremiseSQL.

    Args:
        name: Name for the data premise.
        column: Column to be read by the premise.
        query: SQL query to be used on the validation.
    Attributes:
        type: Type indicator of the premise. It is always "SQL".
    """

    type = PremiseType.SQL

    def __init__(self, name: str, data_node: "DataNode", query: str):
        """Initialize the DataPremiseSQL object."""
        super().__init__(name, data_node)
        self.query = query

    def to_serializeble_dict(self) -> dict:
        return {}
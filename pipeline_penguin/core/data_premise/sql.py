"""This module provides the abstract DataPremiseSQL constructor."""
import pipeline_penguin.core.data_premise
from pipeline_penguin.core.data_node.data_node import DataNode
from . import DataPremise, PremiseType


class DataPremiseSQL(DataPremise):
    """Constructor for the DataPremiseSQL.

    Args:
        name: Name for the data premise.
        column: Column to be read by the premise. # TODO: What's this?
        query: SQL query to be used on the validation.
    Attributes:
        type: Type indicator of the premise. It is always "SQL".
    """

    def __init__(self, name: str, data_node: DataNode, query: str):
        """Initialize the DataPremiseSQL object."""
        super().__init__(name, PremiseType.SQL, data_node)
        self.query = query

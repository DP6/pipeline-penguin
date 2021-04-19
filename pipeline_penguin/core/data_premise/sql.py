"""This module provides the abstract DataPremiseSQL constructor."""
import pipeline_penguin.core.data_premise
from . import DataPremise, PremiseType


class DataPremiseSQL(DataPremise):
    def __init__(self, name, column, query):
    """Constructor for the DataPremiseSQL.

    Args:
        name: name for the data premise
        column: column to be read by the premise
        query: SQL query to be executed for premise validation
    Attributes:
        type: type indicator of the premise. It is always "SQL"
    """
    
        super().__init__(name, PremiseType.SQL, column)
        self.query = query

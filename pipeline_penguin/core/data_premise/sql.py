"""This module provides the abstract DataPremiseSQL constructor."""
import pipeline_penguin.core.data_premise
from . import DataPremise, PremiseType


class DataPremiseSQL(DataPremise):
    def __init__(self, name, column, query):
        """
        Constructor for the data_premise_sql.
        """
        super().__init__(name, PremiseType.SQL, column)
        self.query = query

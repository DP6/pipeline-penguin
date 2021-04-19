"""Premise for checking SQL null values."""

import pipeline_penguin.core.data_premise
from pipeline_penguin.core.data_premise import PremiseType
from pipeline_penguin.core.data_premise.sql import DataPremiseSQL


class DataPremiseSQLCheckNull(DataPremiseSQL):
    """Constructor for the DataPremiseSQLCheckNull.

    Args:
        name: Name of the premise.
        column: Column to be validated
    Attributes:
        query: SQL query to be executed for premise validation
        type: type indicator of the premise. It is always "SQL"
    """

    def __init__(self, name, column):
        """Initialize the DataPremise."""
        super().__init__(name, column, "")

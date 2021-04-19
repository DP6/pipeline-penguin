"""This module provides the abstract DataPremise constructor."""


class DataPremise:
    """Constructor for the data_premise.

    Args:
        name: name for the data premise
        type: type indicator of the premise
        column: column to be read by the premise
    """

    def __init__(self, name, type, column):
        """Initialize the DataPremise object."""
        self.name = name
        self.type = type
        self.column = column

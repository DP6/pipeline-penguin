"""This module provides the abstract DataPremise constructor."""


class DataPremise:
    """A DataPemise represents a single validation to be executed on a given source of data (DataNode).

    Args:
        name: Name for the data premise.
        type: Type indicator of the premise.
        data_node: Reference to the DataNode used in the validation.
    """

    def __init__(self, name: str, data_node: "DataNode"):
        """Initialization of the DataPremise."""
        self.name = name
        self.data_node = data_node

    def validate(self) -> "PremiseOutput":
        """Abstract method for running the validation test."""
        pass

    def to_serializeble_dict(self) -> dict:
        return {}

"""This module provides the abstract Connector constructor."""


class Connector:
    """Constructor for the connector_sql.

    Attributes:
        type: Base connector type
    """

    def __init__(self, type: str):
        """Initialize the Connector constructor."""
        self.type = type

    def run(self):
        """Stub method to be overwritten."""
        pass

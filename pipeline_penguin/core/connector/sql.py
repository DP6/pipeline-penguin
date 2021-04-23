"""This module provides the abstract ConnectorSQL constructor."""

# from ..connector import Connector
from .connector import Connector


class ConnectorSQL(Connector):
    """Constructor for the connector_sql.

    Attributes:
        type: Base connector type (default: SQL)
    """

    type = "SQL"

    def __init__(self):
        """Initialize the ConnectorSQL constructor."""
        super().__init__()

    def run(self, query):
        """Stub method to be overwritten."""
        pass

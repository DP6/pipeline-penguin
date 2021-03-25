from .connector import Connector


class ConnectorSQL(Connector):
    """Constructor for the connector_sql.

    Attributes:
        type: Base connector type (default: SQL)
    """

    def __init__(self):
        super().__init__("SQL")

    def run(self, query):
        pass

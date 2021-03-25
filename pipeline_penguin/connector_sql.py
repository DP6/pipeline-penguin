from .connector import Connector


class ConnectorSQL(Connector):
    def __init__(self):
        """
        Constructor for the connector_sql.
        """
        super().__init__("SQL")

    def run(self, query):
        pass

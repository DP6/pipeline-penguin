"""Contains the `ConnectorSQL` constructor, used for creating new SQL-based Connectors.

The class provided by this module should not be instantiated directly, instead its designed
to be inherited by other classes for more specific SQL-based data sources (i.e. BigQuery, PostGres,
NoSQL, etc).

Location: pipeline_penguin/core/connector/
"""

from .connector import Connector


class ConnectorSQL(Connector):
    """Abstract parent constructor for SQL Connectors.

    Attributes:
        type: Base connector type (constant: "SQL").
    """

    type = "SQL"

    def __init__(self):
        super().__init__()

    def run(self, query: str):
        """Method for executing a SQL query against the related database.
        Args:
            query: SQL query to be executed
        """
        pass

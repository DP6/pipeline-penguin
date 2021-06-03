"""Contains the core `Connector` constructor, used for creating other types Connector classes.

The class provided by this module should not be instantiated directly, instead its designed
to be inherited by other classes for more specific data sources (i.e. BigQuery, GCS, Sheets).

Location: pipeline_penguin/core/connector/

Example usage:

```python
class ConnectorSQL(Connector):

    def __init__(self, params):
        # ...
        # Code for initializing the Connector.
        # ...
        super().__init__()

    def run(self, query):
        # ...
        # Code for executing a sql query against the dtabase.
        # ...
        returns results
```
"""


class Connector:
    """Abstract parent constructor for building other Connector classes."""

    def __init__(self):
        pass

    def run(self):
        """Method for extracting data from the related data source."""
        pass

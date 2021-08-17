"""Contains the `DataNodeBigQuery` constructor, which representings a given table or view from a
BigQuery's dataset.

The DataNode provides information needed for acessing the data on the GCP project and holds the
validations which will be executed against that data. It may also have a custom connector separated
from the Default connectors configured in the ConnectorManager.

Location: pipeline_penguin/data_node/sql

Example usage:

```python
data_node = DataNodeBigQuery(
    "name_test",
    "project_test",
    "dataset_test",
    "table_test",
    "account_test",
)

data_node.insert_premise(
    "check_nulls", DataPremiseSQLCheckIsNull, column="test_column"
)
result = data_node.run_premises()
```
"""

from pipeline_penguin.core.data_node import DataNode, NodeType
from pipeline_penguin.core.data_premise import PremiseType
from pipeline_penguin.connector.connector_manager import ConnectorManager
from pipeline_penguin.exceptions import WrongTypeReference
from pipeline_penguin.connector.sql.bigquery import ConnectorSQLBigQuery


class DataNodeBigQuery(DataNode):
    """Constructor for the DataNodeBigQuery, represents a single table or view from a BigQuery's
    dataset.

    Args:
        name: Name for this datanode.
        project_id: GCP project where the data is stored.
        dataset_id: BigQuery's dataset where the data is stored.
        table_id: BigQuery's table containing the data.
    Attributes:
        name: Name for this datanode.
        project_id: GCP project where the data is stored.
        dataset_id: BigQuery's dataset where the data is stored.
        table_id: BigQuery's table containing the data.
        premises: Dictionary holding every data_premise inserted
        supported_premise_types: Array of premise types allowed to be inserted on the data_node.
        source: Type of data source, it is always "BigQuery".
    """

    def __init__(self, name, project_id, dataset_id, table_id):
        """Initialize the constructor."""
        super().__init__(name, NodeType.BIG_QUERY)
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.table_id = table_id
        self.supported_premise_types = [PremiseType.SQL]

    def get_connector(self, premise_type: str) -> ConnectorSQLBigQuery:
        """Method for retrieving the Connector to be used while querying data from
        this DataNode.

        Calls for a default connector if there's no Connector of the given type inside
        the **self.connectors** dictionary.

        Args:
            premise_type (str): Type of Premise for identifying the Connector.
        Raises:
            WrongTypeReference: If a Connector of the provied type was not found.
        Returns:
            The `Connector` instance retrieved.
        """
        key = f"{premise_type}{self.source}"

        if key in self.connectors:
            return self.connectors[key]

        connector = ConnectorManager().get_default(ConnectorSQLBigQuery)
        if connector is None:
            raise WrongTypeReference(
                f"Could not find {key} as a custom or default connector"
            )

        return connector

    def to_serializeble_dict(self) -> dict:
        """Returns a dictionary representation of the current DataNode using only built-in data
        types.

        Returns:
            dict -> Dicionary containing attributes of this DataNode.
        """
        result = {
            "name": self.name,
            "source": self.source,
            "supported_premise_types": self.supported_premise_types,
            "project_id": self.project_id,
            "dataset_id": self.dataset_id,
            "table_id": self.table_id,
        }

        return result

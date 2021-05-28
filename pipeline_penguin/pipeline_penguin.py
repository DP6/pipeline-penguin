r"""Containst the PipelinePenguin, responsible for centralizing the instances of DataNodes and
Connectors into a single object instance.

Location: pipeline_penguin/

Example usage:

```python
pipe = PipelinePenguin()

# creating a node
pipe.nodes.create_node(
    name="Pipeline X - Table Y",
    node_factory=DataNodeBigQuery,
    project_id= "teste",
    dataset_id= "dataset_test",
    table_id= "table",
    service_account_json= "service_account.json",
)

# Adding a connetor
bq_connector = ConnectorSQLBigQuery(credentials_path="credentials.json")
pipe.connectors.define_default(bq_connector) # Registering bq_connector

```
"""
from .data_node import NodeManager
from .connector import ConnectorManager


class PipelinePenguin:
    def __init__(self):
        """Constructor for instantiating the PipelinePenguin object.

        Attributes:
            nodes: NodeManager instance, storing all DataNode objects for the project
            conectors: ConnetorManager instance, storing the default Connector objects used in the
            project.
        """
        self.nodes = NodeManager()
        self.connectors = ConnectorManager()

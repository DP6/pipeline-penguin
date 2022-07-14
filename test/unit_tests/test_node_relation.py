import pytest

from pipeline_penguin.data_node import NodeManager
from pipeline_penguin.data_node.sql.bigquery import DataNodeBigQuery
from pipeline_penguin.core.data_node.data_node import DataNode
from pipeline_penguin.core.node_relation.node_relation import NodeRelation


@pytest.fixture()
def _data_node():
    bigquery_args = {
        "project_id": "teste",
        "dataset_id": "dataset_test",
        "table_id": "table",
    }

    node_manager = NodeManager()
    yield node_manager.create_node(
        name="Pipeline X - Table Y",
        node_factory=DataNodeBigQuery,
        **bigquery_args,
    )


@pytest.fixture()
class TestNodeRelation:
    def check_add_relation(self, _data_nodeA, _data_nodeB):
        relation = _data_nodeA.add_relation(relation=_data_nodeB, isDestination=True)
        return relation

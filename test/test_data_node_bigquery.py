from pipeline_penguin import PipelinePenguin, NodeType
from pipeline_penguin.node_manager import NodeManager
from pipeline_penguin.data_node import DataNode
from pipeline_penguin.data_node_bigquery import DataNodeBigQuery


class TestDataNodeBigQuery:
    def test_instance_type(self):
        assert isinstance(
            DataNodeBigQuery(
                "name_test",
                "project_test",
                "dataset_test",
                "table_test",
                "account_test",
            ),
            DataNodeBigQuery,
        )

    def test_instance_superclass_type(self):
        assert isinstance(
            DataNodeBigQuery(
                "name_test",
                "project_test",
                "dataset_test",
                "table_test",
                "account_test",
            ),
            DataNode,
        )

    def test_node_type_reference(self):
        assert DataNodeBigQuery == NodeType.BIG_QUERY

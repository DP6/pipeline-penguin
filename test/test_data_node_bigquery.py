from pipeline_penguin.core.data_node import DataNode, NodeType
from pipeline_penguin.data_node import DataNodeBigQuery
from pipeline_penguin.data_premise.sql.check_null import DataPremiseSQLCheckNull


class TestDataNodeBigQuery:
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
        data_node = DataNodeBigQuery(
            "name_test",
            "project_test",
            "dataset_test",
            "table_test",
            "account_test",
        )
        assert data_node.source == NodeType.BIG_QUERY

    def test_insert_premise(self):
        data_node = DataNodeBigQuery(
            "name_test",
            "project_test",
            "dataset_test",
            "table_test",
            "account_test",
        )

        data_node.insert_premise(
            "check_nulls", DataPremiseSQLCheckNull, column="test_column"
        )

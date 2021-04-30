import pytest

from pipeline_penguin.core.data_premise import DataPremise
from pipeline_penguin.core.data_premise.sql import DataPremiseSQL
from pipeline_penguin.core.data_node.data_node import DataNode


@pytest.fixture()
def _data_premise_arguments():
    yield {
        "name": "name_test",
        "data_node": DataNode("node_test", "TEST_SOURCE"),
    }


@pytest.fixture()
def _data_premise_sql_arguments():
    yield {
        "name": "name_test",
        "data_node": DataNode("node_test", "TEST_SOURCE"),
        "column": "test_column",
        "query": "SELECT * FROM `table`",
    }


class TestDataPremise:
    def test_instance_type(self, _data_premise_arguments):
        data_premise = DataPremise(**_data_premise_arguments)
        assert isinstance(data_premise, DataPremise)


class TestDataPremiseSQL:
    def test_instance_type(self, _data_premise_sql_arguments):
        data_premise_sql = DataPremiseSQL(**_data_premise_sql_arguments)
        assert isinstance(data_premise_sql, DataPremise)

    def test_instance_superclass_type(self, _data_premise_sql_arguments):
        data_premise_sql = DataPremiseSQL(**_data_premise_sql_arguments)
        assert isinstance(data_premise_sql, DataPremiseSQL)

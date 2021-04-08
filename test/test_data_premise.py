import pytest

from pipeline_penguin.core.data_premise import DataPremise, DataPremiseSQL


@pytest.fixture()
def data_premise_arguments():
    yield {"name": "name_test", "type": "type_test", "column": "column_test"}


@pytest.fixture()
def data_premise_sql_arguments():
    yield {
        "name": "name_test",
        "type": "type_test",
        "column": "column_test",
        "query": "SELECT * FROM `table`",
    }


class TestDataPremise:
    def test_instance_type(self, data_premise_arguments):
        data_premise = DataPremise(**data_premise_arguments)
        assert isinstance(data_premise, DataPremise)


class TestDataPremiseSQL:
    def test_instance_type(self, data_premise_sql_arguments):
        data_premise_sql = DataPremiseSQL(**data_premise_sql_arguments)
        assert isinstance(data_premise_sql, DataPremise)

    def test_instance_superclass_type(self, data_premise_sql_arguments):
        data_premise_sql = DataPremiseSQL(**data_premise_sql_arguments)
        assert isinstance(data_premise_sql, DataPremiseSQL)

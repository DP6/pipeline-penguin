import pytest

from pipeline_penguin.core.data_node import NodeType
from pipeline_penguin.core.data_premise import DataPremise
from pipeline_penguin.exceptions import WrongTypeReference
from pipeline_penguin.data_node import NodeManager
from pipeline_penguin.core.data_premise.sql import DataPremiseSQL
from pipeline_penguin.data_node.sql.bigquery import DataNodeBigQuery
from pipeline_penguin.core.data_premise.data_premise import DataPremise
from pipeline_penguin.core.data_node.data_node import DataNode


@pytest.fixture()
def _data_node():
    bigquery_args = {
        "project_id": "teste",
        "dataset_id": "dataset_test",
        "table_id": "table",
        "service_account_json": "service_account.json",
    }

    node_manager = NodeManager()
    yield node_manager.create_node(
        name="Pipeline X - Table Y",
        node_factory=DataNodeBigQuery,
        **bigquery_args,
    )


@pytest.fixture()
def _premise_check():
    def check_null():
        class CheckIfNullCreator(DataPremiseSQL):
            def __init__(self, name, data_node: DataNode, column: str, query: str = ""):
                super().__init__(name, data_node, column, query)

            def validate(self):
                return True

        return CheckIfNullCreator

    yield check_null


@pytest.fixture()
def _another_premise_check():
    def another_fake_check():
        class CheckFakeCreator(DataPremiseSQL):
            def __init__(self, name, data_node: DataNode, column: str, query: str = ""):
                super().__init__(name, data_node, column, query)

        return CheckFakeCreator

    yield another_fake_check


@pytest.fixture()
def wrong_premise_check():
    def another_fake_check():
        return DataPremise

    yield another_fake_check


class TestDataNodeInsertPremise:
    def test_if_premise_is_inserted_successfully(self, _data_node, _premise_check):
        premise_name = "Null Checker on Column X"
        _data_node.insert_premise(premise_name, _premise_check(), column="X")

        assert premise_name in _data_node.premises
        assert isinstance(_data_node.premises[premise_name], DataPremise)

    def test_if_raises_exception_when_premise_param_is_not_data_premise_subclass(
        self, _data_node, wrong_premise_check
    ):
        premise_name = "Null Checker on Column Y"

        with pytest.raises(WrongTypeReference) as msg:
            _data_node.insert_premise(
                name=premise_name, premise_factory=wrong_premise_check()
            )

        expected_message = "premise_factory param should be subclass of DataPremise"
        assert str(msg.value) == expected_message

        premise_name = "Null Checker on Column Q"

        with pytest.raises(WrongTypeReference) as msg:
            _data_node.insert_premise(name=premise_name, premise_factory="wrong_type")

        assert str(msg.value) == expected_message

    def test_if_overwrite_premise_if_it_is_already_inserted(
        self, _data_node, _premise_check, _another_premise_check
    ):
        premise_name = "Null Checker on Column X"

        _data_node.insert_premise(
            name=premise_name, premise_factory=_premise_check(), column="X"
        )

        assert premise_name in _data_node.premises
        assert isinstance(_data_node.premises[premise_name], DataPremise)

        _data_node.insert_premise(
            name=premise_name, premise_factory=_another_premise_check(), column="X.x"
        )
        assert _data_node.premises[premise_name].data_node == _data_node


class TestDataNodeRemovePremise:
    def test_if_remove_string_premise(self, _data_node, _premise_check):
        premise_name = "Null Checker on Column X"

        _data_node.insert_premise(
            name=premise_name, premise_factory=_premise_check(), column="X"
        )

        assert premise_name in _data_node.premises
        assert isinstance(_data_node.premises[premise_name], DataPremise)

        _data_node.remove_premise(name=premise_name)
        assert premise_name not in _data_node.premises


class TestDataNodeRunPremise:
    def test_run_premises_without_any_premise(self, _data_node):
        assert _data_node.run_premises() == {}

    def test_run_premises_with_a_single_premise(self, _data_node, _premise_check):

        premise_name = "test_premise"
        _data_node.insert_premise(
            name=premise_name, premise_factory=_premise_check(), column="X"
        )
        assert _data_node.run_premises() == {premise_name: True}

    def test_run_premises_with_many_premises(self, _data_node, _premise_check):
        premise_names = ["premise_a", "premise_b", "premise_c"]

        for name in premise_names:
            _data_node.insert_premise(
                name=name, premise_factory=_premise_check(), column="X"
            )

        expected_result = {name: True for name in premise_names}

        assert _data_node.run_premises() == expected_result

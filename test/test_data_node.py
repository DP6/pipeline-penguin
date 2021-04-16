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
def mock_data_node():
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
def premise_check():
    def check_null():
        class CheckIfNullCreator(DataPremiseSQL):
            def __init__(self, name, data_node: DataNode, column: str, query: str = ""):
                super().__init__(name, data_node, query)

        return CheckIfNullCreator

    yield check_null


@pytest.fixture()
def another_premise_check():
    def another_fake_check():
        class CheckFakeCreator(DataPremiseSQL):
            def __init__(self, name, data_node: DataNode, column: str, query: str = ""):
                super().__init__(name, data_node, query)

        return CheckFakeCreator

    yield another_fake_check


@pytest.fixture()
def wrong_premise_check():
    def another_fake_check():
        return DataPremise

    yield another_fake_check


class TestDataNodeInsertPremise:
    def test_if_premise_is_inserted_successfully(self, mock_data_node, premise_check):
        premise_name = "Null Checker on Column X"
        mock_data_node.insert_premise(premise_name, premise_check(), column="X")

        assert premise_name in mock_data_node.premises
        assert isinstance(mock_data_node.premises[premise_name], DataPremise)

    def test_if_raises_exception_when_premise_param_is_not_data_premise_subclass(
        self, mock_data_node, wrong_premise_check
    ):
        premise_name = "Null Checker on Column Y"

        with pytest.raises(WrongTypeReference) as msg:
            mock_data_node.insert_premise(
                name=premise_name, premise_factory=wrong_premise_check()
            )

        expected_message = "premise_factory param should be subclass of DataPremise"
        assert str(msg.value) == expected_message

        premise_name = "Null Checker on Column Q"

        with pytest.raises(WrongTypeReference) as msg:
            mock_data_node.insert_premise(
                name=premise_name, premise_factory="wrong_type"
            )

        assert str(msg.value) == expected_message

    def test_if_overwrite_premise_if_it_is_already_inserted(
        self, mock_data_node, premise_check, another_premise_check
    ):
        premise_name = "Null Checker on Column X"

        mock_data_node.insert_premise(
            name=premise_name, premise_factory=premise_check(), column="X"
        )

        assert premise_name in mock_data_node.premises
        assert isinstance(mock_data_node.premises[premise_name], DataPremise)

        mock_data_node.insert_premise(
            name=premise_name, premise_factory=another_premise_check(), column="X.x"
        )
        assert mock_data_node.premises[premise_name].data_node == mock_data_node


class TestDataNodeRemovePremise:
    def test_if_remove_string_premise(self, mock_data_node, premise_check):
        premise_name = "Null Checker on Column X"

        mock_data_node.insert_premise(
            name=premise_name, premise_factory=premise_check(), column="X"
        )

        assert premise_name in mock_data_node.premises
        assert isinstance(mock_data_node.premises[premise_name], DataPremise)

        mock_data_node.remove_premise(name=premise_name)
        assert premise_name not in mock_data_node.premises

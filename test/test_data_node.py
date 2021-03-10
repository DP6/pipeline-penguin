from typing import Type

import pytest

from pipeline_penguin import NodeType
from pipeline_penguin.data_node import DataNode
from pipeline_penguin.node_manager import NodeManager


@pytest.fixture()
def data_node():
    bigquery_args = {
        "project_id": "teste",
        "dataset_id": "dataset_test",
        "table_id": "table",
        "service_account_json": "service_account.json",
    }

    node_manager = NodeManager()
    yield node_manager.create_node(
        name="Pipeline X - Table Y",
        node_type=NodeType.BIG_QUERY,
        args=bigquery_args,
    )


class DataPremise:
    pass


@pytest.fixture()
def premise_check():
    def check_null(column_name: str):
        class CheckIfNullCreator(DataPremise):
            def __init__(self, node: Type[DataNode], name: str):
                self.node = node
                self.name = name
                self.column = column_name
                self.query = ""

            def validate(self) -> bool:
                pass

        return CheckIfNullCreator
    yield check_null


def another_fake_check(column_name: str):
    class CheckFakeCreator(DataPremise):
        def __init__(self, node: Type[DataNode], name: str):
            self.node = node
            self.name = name
            self.column = column_name
            self.query = ""

        def validate(self) -> bool:
            pass

    return CheckFakeCreator


@pytest.fixture()
def another_premise_check():
    yield another_fake_check


class TestDataNode:
    def test_if_premise_is_inserted_successfully(self, data_node, premise_check):
        premise_name = "Null Checker on Column X"

        data_node.insert_premise(name=premise_name, premise_factory=premise_check("X"))

        assert premise_name in data_node.premises
        assert isinstance(data_node.premises[premise_name], DataPremise)

    def test_if_overwrite_premise_if_it_is_already_inserted(
        self, data_node, premise_check, another_premise_check
    ):
        premise_name = "Null Checker on Column X"

        data_node.insert_premise(name=premise_name, premise_factory=premise_check("X"))

        assert premise_name in data_node.premises
        assert isinstance(data_node.premises[premise_name], DataPremise)

        data_node.insert_premise(
            name=premise_name, premise_factory=another_premise_check("X.x")
        )
        assert data_node.premises[premise_name].column == "X.x"

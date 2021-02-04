from typing import Type, Optional

from .data_node import DataNode
from .data_node_bigquery import DataNodeBigQuery
from .exceptions import NodeManagerMissingArgs


class NodeManager:
    def __init__(self):
        """
        Constructor for the NodeManager.
        """
        self.nodes = {}

    def create_generic_node(self, name: str, node_type: Type[DataNode], args: dict):
        """

        Initiate a DataNode with the inputted data.
        Returns a DataNode.

        :param name: str
        :param node_type:
        :param args: dict
        :rtype: Type DataNode
        """
        try:
            node = node_type(**args)
        except TypeError as e:
            raise NodeManagerMissingArgs(str(e))

        self.nodes.update({name: node})
        return node

    def create_bigquery_node(self, name: str, args: dict) -> DataNodeBigQuery:
        node = self.create_generic_node(name=name, node_type=DataNodeBigQuery, args=args)
        return node

    def get_node(self, name: str) -> Optional[Type[DataNode]]:
        """
        Get a DataNode that already exists.
        Returns a DataNode.
        """
        return self.nodes.get(name)

    def list_nodes(self) -> list[Type[DataNode]]:
        # TODO discuss about the responsibility of only print
        """
        Prints every DataNode name on the nodes dictionary.
        Returns list of DataNode.
        """
        for key in self.nodes.keys():
            print(key)

        return list(self.nodes.keys())

    def remove_node(self, name: str) -> None:
        """
        Remove a DataNode from the nodes dictionary, then turn it into None.
        Returns None.
        """

        if name in self.nodes:
            del self.nodes[name]

    def copy_node(self, node, name):
        """
        Deep copies a DataNode with a new name.
        Returns a DataNode.
        """
        pass

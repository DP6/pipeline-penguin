import copy
import inspect
from typing import Type, Optional

from .data_node import DataNode
from .exceptions import NodeManagerMissingCorrectArgs, NodeTypeNotFound


class NodeManager:
    _instance = None

    def __new__(cls):
        """
        NodeManager use Singleton Design Pattern
        """
        if not cls._instance:
            cls._instance = super(NodeManager, cls).__new__(cls)
        return cls._instance

    def __init__(self):
        """
        Constructor for the NodeManager.
        Attributes are all privates.
        """
        self.__nodes = {}

    def create_node(self, name: str, node_type: Type[DataNode], args: dict) -> DataNode:
        """
        Initiate a DataNode with the inputted data.
        Returns a DataNode.
        """
        try:
            if inspect.isclass(node_type) and issubclass(node_type, DataNode):
                args.update({"name": name})
                node = node_type(**args)
            else:
                raise NodeTypeNotFound("DataNode should be of type NodeType")
        except TypeError as e:
            raise NodeManagerMissingCorrectArgs(str(e))

        self.__nodes.update({name: node})
        return node

    def get_node(self, name: str) -> Optional[Type[DataNode]]:
        """
        Get a DataNode that already exists.
        Returns a DataNode.
        """
        return self.__nodes.get(name)

    def list_nodes(self) -> list[Type[DataNode]]:
        """
        Prints every DataNode name on the nodes dictionary.
        Returns list of DataNode.
        """
        for key in self.__nodes.keys():
            print(key)

        return list(self.__nodes.keys())

    def remove_node(self, name: str) -> None:
        """
        Remove a DataNode from the nodes dictionary, then turn it into None.
        Returns None.
        """

        if name in self.__nodes:
            del self.__nodes[name]

    def copy_node(self, node: str, name: str) -> DataNode:
        """
        Deep copies a DataNode with a new name.
        Returns a DataNode.
        """
        copied_node = copy.deepcopy(self.__nodes.get(node))
        self.__nodes.update({name: copied_node})

        return copied_node

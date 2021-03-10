import copy
import inspect
from typing import Type, Optional, Union

from .data_node import DataNode
from .exceptions import (
    NodeManagerMissingCorrectArgs,
    NodeTypeNotFound,
    WrongTypeReference,
)


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

    @staticmethod
    def _is_data_node_class(node_type) -> bool:
        return inspect.isclass(node_type) and issubclass(node_type, DataNode)

    @staticmethod
    def _is_data_node_instance(node_type) -> bool:
        return isinstance(node_type, DataNode)

    def create_node(self, name: str, node_type: Type[DataNode], args: dict) -> DataNode:
        """
        Initiate a DataNode with the inputted data.
        Returns a DataNode.
        """
        try:
            if self._is_data_node_class(node_type):
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

    def copy_node(self, node: Union[str, DataNode], name: str) -> Optional[DataNode]:
        """
        Deep copies a DataNode with a new name.
        Returns a DataNode, with the name was not found return None.
        """

        if self._is_data_node_instance(node):
            copied_node = copy.deepcopy(node)
        elif type(node) == str:
            copied_node = copy.deepcopy(self.__nodes.get(node))
        else:
            raise WrongTypeReference(
                "String or DataNode instance should be passed in node type"
            )

        if copied_node:
            copied_node.name = name
            self.__nodes.update({name: copied_node})

            return copied_node

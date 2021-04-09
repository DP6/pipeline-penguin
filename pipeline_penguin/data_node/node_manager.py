"""This module provides the NodeManager constructor."""

import copy
import inspect
from typing import Type, Optional, Union, Any

from pipeline_penguin.core.data_node import DataNode
from pipeline_penguin.exceptions import (
    NodeManagerMissingCorrectArgs,
    WrongTypeReference,
)


class NodeManager:
    """Singleton interface for managing all DataNodes."""

    _instance = None

    def __new__(cls):
        """Magic method for maintaining the singleton pattern."""
        if not cls._instance:
            cls._instance = super(NodeManager, cls).__new__(cls)
        return cls._instance

    def __init__(self):
        """Initialize the manager."""
        self.__nodes = {}

    @staticmethod
    def _is_data_node_class(node_factory: Any) -> bool:
        """Return whether the given class is a DataNode subclass or not.

        Args:
            node_factory: Constructor for a DataNode object
        """
        return (
            node_factory != DataNode
            and inspect.isclass(node_factory)
            and issubclass(node_factory, DataNode)
        )

    def create_node(
        self, name: str, node_factory: Type[DataNode], *args, **kwargs
    ) -> DataNode:
        """Instantiate a DataNode with the proveided arguments.

        Args:
            name: Name of the DataNode
            node_factory: Constructor for a DataNode object
            *args, **kwargs: Arguments used on the DataNode's initialization.
        Raises:
            WrongTypeReference: If the provided constructor is not a subclass of DataNode
            NodeManagerMissingCorrectArgs: When the node was not able to be initialized with the provided arguments
        """
        try:
            if self._is_data_node_class(node_factory):
                node = node_factory(name, *args, **kwargs)
            else:
                raise WrongTypeReference("DataNode should be of type NodeType")
        except TypeError as e:
            raise NodeManagerMissingCorrectArgs(str(e))

        self.__nodes.update({name: node})
        return node

    def get_node(self, name: str) -> Optional[Type[DataNode]]:
        """Retrieve a previously created DataNode by its name.

        Args:
            name: name of the data node to retrieve
        """
        return self.__nodes.get(name)

    def list_nodes(self) -> list[Type[DataNode]]:
        """List all existing data nodes."""
        for key in self.__nodes.keys():
            print(key)

        return list(self.__nodes.keys())

    def remove_node(self, name: str) -> None:
        """Remove a previously added DataNode."""
        if name in self.__nodes:
            del self.__nodes[name]

    def copy_node(self, node: Union[str, DataNode], name: str) -> Optional[DataNode]:
        """Create a copy of the given DataNode with a new name.

        Args:
            node: Primary DataNode
            name: Name of the DataNode copy
        """
        if isinstance(node, DataNode):
            copied_node = copy.deepcopy(node)
        elif isinstance(node, str):
            copied_node = copy.deepcopy(self.__nodes.get(node))
        else:
            raise WrongTypeReference(
                "String or DataNode instance should be passed in node type"
            )

        if copied_node:
            copied_node.name = name
            self.__nodes.update({name: copied_node})

            return copied_node

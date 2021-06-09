"""Contains the `NodeManager` object, responsible for creating, removing and managing DataNode
instances.

The `NodeManager` is a singleton object that provides methods for creating, listing, retrieving and
removing DataNodes.

Location: pipeline_penguin/data_node/

Example usage:

```python
node_manager = NodeManager()
data_node = node_manager.create_node(
    name="Pipeline X - Table Y",
    node_factory=DataNodeBigQuery,
    project_id= "teste",
    dataset_id= "dataset_test",
    table_id= "table",
)

bigquery_args.update({"source": "BigQuery", "premises": {}})

node_manager.remove_node(name="Pipeline X - Table Y")
```
"""
import copy
import inspect
from typing import Type, Optional, Union, Any, List

from pipeline_penguin.core.data_node import DataNode
from pipeline_penguin.exceptions import (
    NodeManagerMissingCorrectArgs,
    WrongTypeReference,
)
from pipeline_penguin.core.premise_output.output_formatter import OutputFormatter
from pipeline_penguin.core.premise_output.output_manager import OutputManager


class NodeManager:
    """Singleton responsible for creating, listing, retrieving and removing DataNodes.

    Attributes:
        __nodes: Protected dictionary for storing the added DataNodes.
    """

    _instance = None

    def __new__(cls):
        """Magic method for maintaining the singleton pattern."""
        if not cls._instance:
            cls._instance = super(NodeManager, cls).__new__(cls)
        return cls._instance

    def __init__(self):
        self.__nodes = {}

    @staticmethod
    def _is_data_node_class(node_factory: Any) -> bool:
        """Method for checking if a given object is of a `DataNode` class constructor.

        Args:
            node_factory: Any object.
        Returns:
            A `boolean` value indicating whether the provied object is a `DataNode` class or not.
        """
        return (
            node_factory != DataNode
            and inspect.isclass(node_factory)
            and issubclass(node_factory, DataNode)
        )

    def create_node(
        self, name: str, node_factory: Type[DataNode], *args, **kwargs
    ) -> DataNode:
        """Method for instantiating a new DataNode inside the internal data structure.

        Args:
            name: Name of the DataNode
            node_factory: Constructor for a DataNode object
            *args, **kwargs: Arguments used on the DataNode's initialization.
        Raises:
            WrongTypeReference: If the provided constructor is not a subclass of DataNode
            NodeManagerMissingCorrectArgs: When the node was not able to initialize the provided
            arguments
        Returns:
            The newly created `DataNode` instance.
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
        """Method for retrieving a DataNode by its name.

        Args:
            name: Name of the DataNode to retrieve
        Returns:
            The identified `DataNode` or `None` if it does not exist
        """
        return self.__nodes.get(name)

    def list_nodes(self) -> List[Type[DataNode]]:
        """Method for listing previously created DataNode instances.

        Returns:
            A `list` of strings representing the name attribute of previously added `DataNode`
            instances.
        """
        for key in self.__nodes.keys():
            print(key)

        return list(self.__nodes.keys())

    def remove_node(self, name: str) -> None:
        """Method for deleting an previously added DataNode instance given its name.

        Args:
            name: Name of the DataNode to be removed.
        Returns:
            `None`
        """
        if name in self.__nodes:
            del self.__nodes[name]

    def copy_node(self, node: Union[str, DataNode], name: str) -> Optional[DataNode]:
        """Method for copying a DataNode and adding the copy to the NodeManager's internal
        dictionary.

        Args:
            node: Name (string) or instance of the DataNode to be copied. If a name is provided we
            try to retrieve the DataNode's instance using the self.get method.
            name: Name for the DataNode copy.
        Raises:
            WrongTypeReference: If the `node` argument is not a string or a DataNode instance.
        Returns:
            The `DataNode` copy instance.
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

    def run_premises(self) -> OutputManager:
        """Run DataPremise validations for every DataNode registered on the internal __nodes
        dictionary while storing their results on a OutputManager object.

        Returns:
            A `OutPutManager` instance consolidating all validation results on its "output"
            attribute
        """
        output_manager = OutputManager()
        for name, data_node in self.__nodes.items():
            results = data_node.run_premises()
            output_manager.outputs[name] = results

        return output_manager

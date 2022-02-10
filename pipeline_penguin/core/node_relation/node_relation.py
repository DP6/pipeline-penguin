"""node_relation module, provides the NodeRelation class, which represents a directional relationship between two DataNodes from the pipeline.

DataNode objects can be related as the source or destination of a data transformation. The NodeRelation object is used to abstract this relationship in the validation model for the whole pipeline.

Example usage:

```python
data_node_a = DataNodeBigQuery()
data_node_b = DataNodeBigQuery()

NodeRelation(data_node_a, data_node_b)
```
"""


from pipeline_penguin.core.data_node.data_node import DataNode


class NodeRelation:
    """Represents a directional relationship between two DataNodes from the pipeline.
    
    Raises:
        RecursiveRelationException: If the source is the same DataNode as the destination
    Args:
        source: DataNode where the data is read from
        destination: DataNde where the data is written to
    """
    def __init__(self, source: DataNode, destination: DataNode) -> None:
        pass

    def sort_relations() -> list(DataNode):
        """Returns a list with both DataNodes of the relationship "source" as the first element and "destination" as the second
        Returns: 
            List[DataNode]: List of DataNodes from the relation
        """
        pass
    
    def get_source() -> DataNode:
        """
        Returns:
            DataNode: The source DataNode
        """
        pass
    
    def get_destination() -> DataNode:
        """
        Returns:
            DataNode: The destination DataNode
        """
        pass
    
    def set_source(data_node: DataNode) -> None:
        """Updates the value for the source DataNode

        Args:
            data_node (DataNode): The DataNode to override the source
        """
        pass
    
    def set_destination(data_node: DataNode) -> None:
        """Updates the value for the destination DataNode

        Args:
            data_node (DataNode): The DataNode to override the destination
        """
        pass
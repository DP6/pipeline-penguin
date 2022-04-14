"""node_relation module, provides the NodeRelation class, which represents a directional relationship between two DataNodes from the pipeline.

DataNode objects can be related as the source or destination of a data transformation. The NodeRelation object is used to abstract this relationship in the validation model for the whole pipeline.

Example usage:

```python
data_node_a = DataNodeBigQuery()
data_node_b = DataNodeBigQuery()

NodeRelation(data_node_a, data_node_b)
```
"""
from pipeline_penguin.exceptions import WrongTypeReference
#from pipeline_penguin.core.data_node.data_node import DataNode


class NodeRelation:
    """Represents a directional relationship between two DataNodes from the pipeline.
    
    Raises:
        RecursiveRelationException: If the source is the same DataNode as the destination
    Args:
        source: DataNode where the data is read from
        destination: DataNde where the data is written to
    """
    def __init__(self, source: "DataNode", destination: "DataNode") -> None:
        self.source = source
        self.destination = destination
        
   
    def get_source(self) -> "DataNode":
        """
        Returns:
            DataNode: The source DataNode
        """
        return self.source
    
    def get_destination(self) -> "DataNode":
        """
        Returns:
            DataNode: The destination DataNode
        """
        return self.destination
    
    def set_source(self,data_node: "DataNode") -> None:
        """Updates the value for the source DataNode

        Args:
            data_node (DataNode): The DataNode to override the source
        """

        if data_node != self.destination:
            self.source = data_node
        else:
            raise WrongTypeReference(
                "Source should be different of Destination"
            )
        
        return self.source 
        
    
    def set_destination(self,data_node: "DataNode") -> None:
        """Updates the value for the destination DataNode

        Args:
            data_node (DataNode): The DataNode to override the destination
        """
        if data_node != self.source:
            self.destination = data_node
        else:
            raise WrongTypeReference(
                "Destination should be different of Source"
            )

        return self.destination
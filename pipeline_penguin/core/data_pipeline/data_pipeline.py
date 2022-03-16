"""data_pipeline module, provides the DataPipeline class used for grouping DataNodes into a
validation branch.

Example usage:
```python
data_node_a = DataNodeBigQuery()
data_node_b = DataNodeBigQuery()
data_node_c = DataNodeBigQuery()


pipeline_a = DataPipeline('pipeline_a')
pipeline_a.append(data_node_a)
pipeline_a.append(data_node_b)

# Runs only premises from data_node_a and data_node_b
pipeline_a.run_premises 
```
"""

from typing import List


class DataPipeline:
    """Used for grouping and interacting with only a given subset of data nodes.
    
    Args:
        name: Name for the DataPipeline
    Attributes:
        name: Name for the DataPipeline
        nodes: List of nodes registered on the DataPipeline
    """
    
    def __init__(self, name: str):
        self.name = name
        self.nodes = []
        
    def sort_relations() -> List["DataNode"]:
        """Returns a list of data nodes recursively
        sorted by their relationships (via NodeRelations).
        
        Returns:
            List[DataNode]: List of data nodes
        """
        pass
    
    def append(node: "DataNode"):
        """Adds a DataNode to the DataPipeline

        Args:
            node (DataNode): DataNode to be added
        """
        pass
    
    def run_premises() -> "OutputManager":
        """Runs the premises for all DataNoded added to this DataPipeline

        Returns:
            OutputManager: Manager containing the validation results
        """
        pass
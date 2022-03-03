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
from .data_nodes.node_manager import NodeManager


class PipelinePenguin:
    def __init__(self):
        """
        Constructor for the PipelinePenguin.
        """
        self.nodes = NodeManager()

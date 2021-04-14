from .data_node import NodeManager
from .connector import ConnectorManager


class PipelinePenguin:
    def __init__(self):
        """
        Constructor for the PipelinePenguin.
        """
        self.nodes = NodeManager()
        self.connectors = ConnectorManager()

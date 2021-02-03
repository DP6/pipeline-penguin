class DataNode:
    def __init__(self, name, source):
        """
        Constructor for the DataNode.
        Must check if the source is a valid, supported data source.
        """
        self.name = name
        self.source = source

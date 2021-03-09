class NodeManagerMissingCorrectArgs(TypeError):
    """
    Raised when there are missing arguments in DataNode __init__
    """

    pass


class NodeTypeNotFound(KeyError):
    """
    Raised when node type was not found
    """

    pass


class NodeReferenceWrongType(Exception):
    """
    Raised when node reference used to deep copy is the wrong type
    """

    pass
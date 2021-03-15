class NodeManagerMissingCorrectArgs(TypeError):
    """
    Raised when there are missing arguments in DataNode __init__
    """

    pass


class WrongTypeReference(Exception):
    """
    Raised when reference is the wrong type
    """

    pass

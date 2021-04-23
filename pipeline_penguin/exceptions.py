class NodeManagerMissingCorrectArgs(TypeError):
    """
    Raised when there are missing arguments in DataNode __init__
    """

    pass


class ConnectorManagerMissingCorrectArgs(TypeError):
    """
    Raised when there are missing arguments in ConnectorManager create_connector
    """

    pass


class WrongTypeReference(Exception):
    """
    Raised when reference is the wrong type
    """

    pass

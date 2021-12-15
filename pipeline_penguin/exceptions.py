r"""This module contains custom Exceptions to be used on the internal components of the
pipeline_penguin.

Location: pipeline_penguin/

Example usage:

```python

raise WrongTypeReference("DataNode should be of type NodeType")

```

"""


class NodeManagerMissingCorrectArgs(TypeError):
    """Raised when there are missing arguments in DataNode __init__."""

    pass


class ConnectorManagerMissingCorrectArgs(TypeError):
    """Raised when there are missing arguments in ConnectorManager create_connector."""

    pass


class WrongTypeReference(Exception):
    """Raised when reference is the wrong type."""

    pass

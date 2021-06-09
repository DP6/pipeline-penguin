"""Contains the `ConnectorManager` object responsible for defining and managing the default
connectors.

Default connectors are used "by default" according to the NodeType and PremiseType of the
current premise execution

Location: pipeline_penguin/connector/

Example usage:

```python
connector_manager = ConnectorManager()
bq_connector = ConnectorSQLBigQuery(credentials_path="credentials.json")

connector_manager.define_default(bq_connector) # Registering bq_connector
connector_manager.get_default(ConnectorSQLBigQuery) # Returns bq_connector
connector_manager.remove_default(ConnectorSQLBigQuery) # Deletes bq_connector
```
"""
import inspect
from typing import Type, Any
from pipeline_penguin.core.connector import Connector
from pipeline_penguin.exceptions import (
    WrongTypeReference,
    ConnectorManagerMissingCorrectArgs,
)


class ConnectorManager:
    """Singleton responsible for registering default connectors used for communication with
    the data sources.

    Attributes:
        __default_connectors: Internal data structure for storing registered Connectors.
    """

    _instance = None
    __default_connectors = {}

    def __new__(cls):
        """Used for maintaining a single instance of the Manager"""
        if not cls._instance:
            cls._instance = super(ConnectorManager, cls).__new__(cls)
        return cls._instance

    def reset(self):
        self._instance = None
        self.__default_connectors = {}

    @staticmethod
    def _is_connector_subclass(connector_type: Any) -> bool:
        """Used for validating if an object is a Connector instance
        Args:
            connector_type: any object
        Returns:
            A `boolean` that says whether the given object is a Connector instance or not.
        """

        return (
            connector_type != Connector
            and inspect.isclass(connector_type)
            and issubclass(connector_type, Connector)
        )

    def _get_dict_key(self, connector: Connector):
        """Produces a key for identification of the registered the Connector using a combination of
        its "type" and "source".

        Args:
            connector: The Connector to be identified/registered on the internal data strucutre.
        Returns:
            A `string` generated from the given connetor to be used as a dictionary key.
        Raises:
            `ConnectorManagerMissingCorrectArgs` if the provided argument is not a Connector type
            object.
        """
        try:
            if self._is_connector_subclass(
                connector.__class__
            ) or self._is_connector_subclass(connector):
                return connector.type + connector.source
        except TypeError as e:
            raise ConnectorManagerMissingCorrectArgs(str(e))

    def define_default(self, connector: Connector) -> None:
        """Method for registering a given connetor in the connector manager.

        Args:
            connector: The Connector to be identified/registered on the internal data strucutre.
        Returns:
            None
        Raises:
            `ConnectorManagerMissingCorrectArgs` if the provided argument is not a Connector type
        """
        try:
            if self._is_connector_subclass(connector.__class__):
                self.__default_connectors.update(
                    {self._get_dict_key(connector=connector): connector}
                )
            else:
                raise WrongTypeReference("Connector should be of type Connector")
        except TypeError as e:
            raise ConnectorManagerMissingCorrectArgs(str(e))

        return None

    def get_default(self, connector: Connector):
        """Method for obtaining a previously registered connector of the same type and source of
        the given _Connector class_.

        Args:
            connector: The Connector object where the key will be derived from.
        Returns:
            The `Connector` object related to the constructed key or `None` if no connector is
            found.
        """
        return self.__default_connectors.get(self._get_dict_key(connector))

    def remove_default(self, connector: Connector):
        """Method for deleting a previously registered connector of the same type and source of
        the given _Connector class_.

        Args:
            connector: Connector class for the key to be extracted
        Returns:
            The `Connector` object related to the constructed key or `None` if no connector is
            found.
        """

        dict_key = self._get_dict_key(connector)
        if dict_key in self.__default_connectors:
            removed_connector = self.__default_connectors[dict_key]
            del self.__default_connectors[dict_key]
            return removed_connector

        return None

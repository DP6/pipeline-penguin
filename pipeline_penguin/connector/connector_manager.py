""""""
import inspect
from typing import Type, Any
from pipeline_penguin.core.connector import Connector
from pipeline_penguin.exceptions import WrongTypeReference, ConnectorManagerMissingCorrectArgs



class ConnectorManager:
    _instance = None

    def __new__(cls):
        """
        ConnectorManager use Singleton Design Pattern.
        """
        if not cls._instance:
            cls._instance = super(ConnectorManager, cls).__new__(cls)
        return cls._instance

    def __init__(self):
        """
        Constructor for the ConnectorManager.
        Attributes are all privates.
        """
        self.__default_connectors = {}

    @staticmethod
    def _is_connector_subclass(connector_type: Any) -> bool:
        """
        Verify with class passed is child of Connector class
        """
        return (
            connector_type != Connector
            and inspect.isclass(connector_type)
            and issubclass(connector_type, Connector)
        )

    def _get_dict_key(self, **kwargs):
        try:
            if 'connector' in kwargs and self._is_connector_subclass(kwargs['connector'].__class__):
                connector = kwargs['connector']
                return connector.type + connector.source
            elif 'connector_type' in kwargs and self._is_connector_subclass(kwargs['connector_type']):
                connector_type = kwargs['connector_type']
                return connector_type.type + connector_type.source
            elif 'type' in kwargs and isinstance(kwargs['type'], str) and 'source' in kwargs and isinstance(kwargs['source'], str):
                return kwargs['type'] + kwargs['source']
            else:
                raise ConnectorManagerMissingCorrectArgs("Invalid arguments")
        except TypeError as e:
            raise ConnectorManagerMissingCorrectArgs(str(e))

    def define_default(self, connector: Connector) -> None:
        """
        """
        try:
            if self._is_connector_subclass(connector.__class__):
                self.__default_connectors.update({self._get_dict_key(connector = connector): connector})
            else:
                raise WrongTypeReference("Connector should be of type Connector")
        except TypeError as e:
            raise ConnectorManagerMissingCorrectArgs(str(e))

        return None

    def get_default(self, **kwargs):
        """
        """
        return self.__default_connectors.get(self._get_dict_key(**kwargs))

    def remove_default(self, **kwargs):
        """
        """
        dict_key = self._get_dict_key(**kwargs)
        if dict_key in self.__default_connectors:
            removed_connector = self.__default_connectors[dict_key]
            del self.__default_connectors[dict_key]
            return removed_connector
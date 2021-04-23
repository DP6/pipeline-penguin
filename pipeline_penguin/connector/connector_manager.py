""""""

from pipeline_penguin.core.connector import Connector


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

    def create_connector(self, type: str, source: str, args: dict):
        """
        Instantiate the properly Connector considering the type and source.
        Returns the Connector instance.
        """
        pass

    def define_default(self, connector: Connector):
        """
        Define the Connector received as parameter as default for its type and source attributes.
        Returns None.
        """
        pass

    def get_default(self, type: str, source: str):
        """
        Getter for the default Connector for the type and source parameters.
        Returns the default Connector or None if there is no default setted.
        """
        pass

    def remove_default(self, type: str, source: str):
        """
        Remove the default Connector for the type and source parameters.
        Returns the removed Connector or None if there is no default setted.
        """
        pass

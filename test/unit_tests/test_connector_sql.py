from pipeline_penguin.core.connector import Connector, ConnectorSQL


class TestConnectorSQL:
    def test_instance_class_type(self):
        assert isinstance(ConnectorSQL(), ConnectorSQL)

    def test_instance_superclass_type(self):
        assert isinstance(ConnectorSQL(), Connector)

    def test_connector_type(self):
        conn = ConnectorSQL()
        assert conn.type == "SQL"

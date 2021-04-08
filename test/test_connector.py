from pipeline_penguin.core.connector import Connector


class TestConnector:
    def test_instance_class_type(self):
        assert isinstance(Connector("test_type"), Connector)

    def test_connector_type(self):
        conn = Connector("test_type")
        assert conn.type == "test_type"

from pipeline_penguin.core.connector import Connector


class TestConnector:
    def test_instance_class_type(self):
        assert isinstance(Connector(), Connector)

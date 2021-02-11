from pipeline_penguin import PipelinePenguin
from pipeline_penguin.node_manager import NodeManager
from pipeline_penguin.data_node import DataNode


class TestDataNode:
    def test_instance_type(self):
        assert isinstance(DataNode("test", "test"), DataNode)

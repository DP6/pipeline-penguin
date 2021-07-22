from pipeline_penguin import PipelinePenguin
from pipeline_penguin.data_node.node_manager import NodeManager


class TestPipelinePenguin:
    def test_instance_type(self):
        assert isinstance(PipelinePenguin(), PipelinePenguin)

    def test_nodes_type(self):
        assert isinstance(PipelinePenguin().nodes, NodeManager)

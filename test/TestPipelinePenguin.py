import pipeline_penguin
from pipeline_penguin import pipeline_penguin
from pipeline_penguin.NodeManager import NodeManager

class TestPipelinePenguin:
  def test_instance_type(self):
    assert isinstance(pipeline_penguin(), pipeline_penguin)

  def test_nodes_type(self):
    assert isinstance(pipeline_penguin().nodes, NodeManager)
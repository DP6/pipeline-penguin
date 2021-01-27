class NodeManager:
  def __init__(self):
    '''
      Constructor for the NodeManager.
    '''
    self.nodes = {}

  def create_node(self, name, DataNode):
    '''
      Initiate a DataNode with the inputed data.
      Returns a DataNode.
    '''
    pass

  def get_node(self, name):
    '''
      Get a DataNode that already exists.
      Returns a DataNode.
    '''
    pass

  def list_nodes(self):
    '''
      Prints every DataNode name on the nodes dictionary.
      Returns None.
    '''
    pass

  def remove_node(self, name):
    '''
      Remove a DataNode from the nodes dictionary, then turn it into None.
      Returns None.
    '''
    pass
  
  def copy_node(self, node, name):
    '''
      Deep copies a DataNode with a new name.
      Returns a DataNode.
    '''
    pass
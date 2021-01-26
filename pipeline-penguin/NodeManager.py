class NodeManager:
  def __init__(self):
    '''
      Constructor for the NodeManager.
    '''
    self.nodes = {}

  def create_node(self):
    '''
      Initiate a DataNode with the inputed data.
      Returns a DataNode.
    '''
    pass

  def remove_node(self):
    '''
      Receive a DataNode then remove it from the nodes dictionary.
      Returns None.
    '''
    pass
  
  def duplicate_node(self):
    '''
      Receive a DataNode then deep copied it, altering the name to include "_1" at the end and inserting it to the nodes dictionary.
      Returns the new DataNode.
    '''
    pass
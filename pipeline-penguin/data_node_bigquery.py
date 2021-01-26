class data_node_bigquery(DataNode):
  def __init__(self, name, source, project_id, dataset_id, table_id):
    '''
      Constructor for the data_node_bigquery.
      Must receive BigQuery table data.
    '''
    super().__init__(self, name, source)
    self.project_id = project_id
    self.dataset_id = dataset_id
    self.table_id = table_id
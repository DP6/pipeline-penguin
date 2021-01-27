class data_node_bigquery(DataNode):
  def __init__(self, name, project_id, dataset_id, table_id, service_account_json):
    '''
      Constructor for the data_node_bigquery.
      Must receive BigQuery table data.
    '''
    super().__init__(self, name, "BigQuery")
    self.project_id = project_id
    self.dataset_id = dataset_id
    self.table_id = table_id
    self.service_account_json = service_account_json
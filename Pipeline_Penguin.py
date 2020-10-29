from google.cloud import bigquery
import pandas as pd

class Pipeline_Penguin:
  def __init__(self, service_account):
    self._validation_engine = PP_Validation_Engine(service_account)

  def create_validation_node(self, project_id, dataset_id, table_id):
    node = PP_Validation_Node(project_id, dataset_id, table_id, self._validation_engine)
    self.validation_nodes.append(node)
    return node
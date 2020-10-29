class PP_Validation_Node:
  def __init__(self, project_id, dataset_id, table_id, validation_engine):
    self.project_id = project_id
    self.dataset_id = dataset_id,
    self.table_id = table_id,
    self.table_address = "{}.{}.{}".format(project_id, dataset_id, table_id)
    self._validation_engine = validation_engine

    self.table_schema = self._validation_engine.get_table_schema(project_id, dataset_id, table_id)

  def custom_query(self, query):
    custom_query_result = self._validation_engine.query(query)
    temp_project_id = custom_query_result.destination.project
    temp_dataset_id = custom_query_result.destination.dataset_id
    temp_table_id = custom_query_result.destination.table_id
    temp_table_adress = "{}.{}.{}".format(temp_project_id, temp_dataset_id, temp_table_id)
    temp_schema = custom_query_result.result().schema
    columns = ""

    for index, column in enumerate(temp_schema):
      columns +="\n  LOGICAL_AND({})".format(column.name)
      if index != len(temp_schema) - 1:
        columns += ","
    validation_query = "SELECT{}\nFROM\n  {}".format(columns, temp_table_adress)
    result_df = self._validation_engine.query(validation_query).result().to_dataframe()
    self._return_result(result_df)

  def check_null(self):
    columns = ""
    for index, column in enumerate(self.table_schema):
      columns +="\n  LOGICAL_AND({} IS NOT NULL)".format(column.name, str(index))
      if index != len(self.table_schema) - 1:
        columns += ","
    query = "SELECT{}\nFROM\n  {}".format(columns, self.table_address)
    result_df = self._validation_engine.query(query).result().to_dataframe()
    self._return_result(result_df)

  def _return_result(self, result_df):
    if result_df.transpose()[0].min():
      print("Validation Completed. No error found")
    else :
      print("Validation Completed. One or more errors was found on your query")
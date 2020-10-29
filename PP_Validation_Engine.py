class PP_Validation_Engine:
  dry_run = True
  dry_run_limit = 1

  def __init__(self, service_account):
    self.bq_client = bigquery.Client.from_service_account_json(service_account)

  def query(self, query):
    allow_query = self._dry_run_validation(query)
    if allow_query:
      return self.bq_client.query(query)
    return None

  def _dry_run_validation(self, query):
    job_config = bigquery.QueryJobConfig(dry_run = True)
    billed_bytes = self.bq_client.query(query, job_config = job_config).total_bytes_billed
    if billed_bytes > self.dry_run_limit:
      print("This validation bill {} bytes, above the set limit {}. Do you wish to continue? (Y/N)").format(billed_bytes, self.dry_run_limit)
      prompt_anwser = input() 
      while prompt_anwser != "Y" or prompt_anwser != "N":
        print("Invalid anwser. Please input Y or N")
      if prompt_anwser == "N":
        return False
    return True

  def get_table_schema(self, project_id, dataset_id, table_id):
    dataset_ref = self.bq_client.dataset(dataset_id, project = project_id)
    table_ref = dataset_ref.table(table_id)
    table = self.bq_client.get_table(table_ref)
    return table.schema
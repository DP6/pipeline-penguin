class ConnectorSQLBigQuery(ConnectorSQL):
    def __init__(self, credentials_path: str, max_rows: int = 1000):
        """
        Constructor for the connector_sql_bigquery.
        Must receive a path to a service account JSON, and optionally may receive an value for the max_rows attribute, by default it must be 1000.
        """
        super().__init__(self)
        self.source = "BigQuery"
        self.credentials_path = credentials_path
        self.max_rows = max_rows

    def run(self, query: str, max_rows: int = self.max_rows):
        """
        Must receive a SQL query.
        Executes the recieved query on BigQuery using the bigquery python API and the service account at the credentials_path property.
        Must return an Pandas Dataframe with the rows limited by the max_rows parameter.
        """
        pass

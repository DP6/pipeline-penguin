import pandas as pd
from google.oauth2.service_account import Credentials
from os import path

from .connector_sql import ConnectorSQL


class ConnectorSQLBigQuery(ConnectorSQL):
    def __init__(self, credentials_path: str, max_rows: int = 1000):
        """
        Constructor for the connector_sql_bigquery.
        Must receive a path to a service account JSON, and optionally may receive an value for the max_rows attribute, by default it must be 1000.
        """
        super().__init__()
        self.source = "BigQuery"

        if not (path.isfile(credentials_path)):
            raise FileNotFoundError(f"{credentials_path} does not exist")

        self.credentials_path = credentials_path
        self.max_rows = max_rows

    def run(self, query: str, max_rows: int = None):
        """
        Must receive a SQL query.
        Executes the recieved query on BigQuery using the bigquery python API and the service account at the credentials_path property.
        Must return an Pandas Dataframe with the rows limited by the max_rows parameter.
        """

        # Using default max_rows
        max_rows = max_rows if max_rows else self.max_rows

        credentials = Credentials.from_service_account_file(self.credentials_path)

        df = pd.read_gbq(query=query, credentials=credentials, max_results=max_rows)

        return df

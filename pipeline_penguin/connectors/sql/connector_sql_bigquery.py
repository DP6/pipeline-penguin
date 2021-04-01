"""This module provides the ConnectorSQLBigQuery constructor."""

from os import path

import pandas as pd
from google.oauth2.service_account import Credentials

from .connector_sql import ConnectorSQL


class ConnectorSQLBigQuery(ConnectorSQL):
    """Constructor for the connector_sql_bigquery.

    Args:
        credentials_path: Path to a service account JSON file
        max_results: Default maximum row count for the resulting pandas dataframe (default: 1000)
    Attributes:
        type: Base connector type (constant: "SQL")
        source: Source type (constant: "BigQuery")
    Raises:
        FileNotFoundError: If the provided credentials_path is invalid.
    """

    def __init__(self, credentials_path: str, max_results: int = 1000):
        """Initialize the connector."""
        super().__init__()
        self.source = "BigQuery"

        if not (path.isfile(credentials_path)):
            raise FileNotFoundError(f"{credentials_path} does not exist")

        self.credentials_path = credentials_path
        self.max_results = max_results

    def run(self, query: str, max_results: int = None):
        """Retrieve the results of the provided query.

        Args:
            query: SQL code in BigQuery's standard foramt. Ref: https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax
            max_results: max row count for the resulting pandas dataframe. Uses the default value when omitted.
        Returns:
            A pandas DataFrame object with the results of the provided query up to the maximum number of rows allowed.
        """
        # Using default max_results
        max_results = max_results if max_results else self.max_results

        credentials = Credentials.from_service_account_file(self.credentials_path)

        df = pd.read_gbq(query=query, credentials=credentials, max_results=max_results)

        return df

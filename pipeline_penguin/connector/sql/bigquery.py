"""Contains the `ConnectorSQLBigQuery` object responsible for interfacing the communication with
the bigquery GCP service.

The BigQuery connector uses the pandas GBQ library for running queries against a BigQuery database.
It requires the provision of a service account key file in json format.

Location: pipeline_penguin/connector/sql

Example usage:

```python
bq_connector = ConnectorSQLBigQuery(credentials_path="credentials.json")

query_results = bq_connector.run("SELECT * FROM `my_project.my_dataset.my_table`", max_results=500)
```
"""

from os import path

import pandas as pd
from google.oauth2.service_account import Credentials
import google.auth

from pipeline_penguin.core.data_node import NodeType
from pipeline_penguin.core.connector.sql import ConnectorSQL


class ConnectorSQLBigQuery(ConnectorSQL):
    """Object responsible for interfacing the communication with BigQuery data sources.

    Args:
        credentials_path: Path to a service account JSON file.
        max_results: Default maximum row count for the resulting pandas dataframe (default: 1000).
    Attributes:
        type: Base connector type, derived from the ConnectorSQL parent class (constant: "SQL").
        source: Source type (constant: "BigQuery").
        credentials_path: Path to a service account JSON file.
        max_results: Default maximum row count for the resulting pandas dataframe (default: 1000).
    Raises:
        FileNotFoundError: If the file located in the provided credentials_path is invalid or
                           cannot be accessed.
    """

    source = NodeType.BIG_QUERY

    def __init__(self, credentials_path: str = "default", max_results: int = 1000):
        super().__init__()

        print(path.isfile)
        if credentials_path == "default":
            print("Using google.auth.default credentials")
            self.credentials, self.project_id = google.auth.default()
        elif path.isfile(credentials_path):
            self.credentials = Credentials.from_service_account_file(credentials_path)
            self.project_id = None
        else:
            raise FileNotFoundError(f"{credentials_path} does not exist")

        self.max_results = max_results

    def run(self, query: str, max_results: int = None):
        """Method for executing a query and retrieving its results.

        Args:
            query: SQL code in BigQuery's standard format. Reference:
                   https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax
            max_results: Max row count for the resulting pandas dataframe. Uses the default when
                         not provided.
        Returns:
            A pandas `DataFrame` object with the results of the provided query up to the
            maximum number of rows allowed.
        """
        # Using default max_results
        max_results = max_results if max_results else self.max_results

        df = pd.read_gbq(
            query=query,
            credentials=self.credentials,
            max_results=max_results,
            project_id=self.project_id,
        )

        return df

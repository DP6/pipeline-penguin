import pytest

# Imports for mocking
import pandas as pd
from google.oauth2.service_account import Credentials
from os import path

# Pipeline Penguing
from pipeline_penguin import PipelinePenguin
from pipeline_penguin.connector.sql.bigquery import ConnectorSQLBigQuery
from pipeline_penguin.premise_output.output_formatter_log import OutputFormatterLog
from pipeline_penguin.data_node.sql.bigquery import DataNodeBigQuery
from pipeline_penguin.data_premise.sql.check_null import DataPremiseSQLCheckIsNull
from pipeline_penguin.data_premise.sql.check_distinct import DataPremiseSQLCheckDistinct
from pipeline_penguin.data_premise.sql.check_regexp import (
    DataPremiseSQLCheckRegexpContains,
)
from pipeline_penguin.data_premise.sql.check_in import DataPremiseSQLCheckInArray


@pytest.fixture
def _mock_from_service_account_file(monkeypatch):
    def mock_from_service_account_file(credentials_path):
        return

    monkeypatch.setattr(
        Credentials, "from_service_account_file", mock_from_service_account_file
    )


@pytest.fixture
def _mock_is_file(monkeypatch):
    def mock_is_file(path):
        return path == "./service_acc.json"

    monkeypatch.setattr(path, "isfile", mock_is_file)


@pytest.fixture
def _mock_pandas_read_gbq(monkeypatch):
    def mock_pandas_read_gbq(query, credentials, max_results):
        return pd.DataFrame([[0, 2]], columns=["total", "result"])

    monkeypatch.setattr(pd, "read_gbq", mock_pandas_read_gbq)


class TestFullExecution:
    def test_everything(
        self, _mock_from_service_account_file, _mock_is_file, _mock_pandas_read_gbq
    ):
        pp = PipelinePenguin()

        gcp_service_acc = "./service_acc.json"
        bq_conn = ConnectorSQLBigQuery(gcp_service_acc)
        pp.connectors.define_default(bq_conn)
        log_formatter = OutputFormatterLog()

        bq_conversions = pp.nodes.create_node(
            "Conversions",
            DataNodeBigQuery,
            project_id="dp6-estudos",
            dataset_id="test_pipeline_penguin",
            table_id="vw_conversions",
        )

        bq_conversions.insert_premise(
            "All transactions have revenue",
            DataPremiseSQLCheckIsNull,
            column="transactionRevenue",
        )

        bq_conversions.insert_premise(
            "All session ids are distinct",
            DataPremiseSQLCheckDistinct,
            column="session_id",
        )

        bq_conversions.insert_premise(
            "TransactionID is in the correct format",
            DataPremiseSQLCheckRegexpContains,
            column="transactionId",
            pattern=r"^ORD\\d{12}$",
        )

        # Running premise and printing the results
        validation_results = bq_conversions.run_premises()

        print(
            log_formatter.export_output(
                validation_results["All transactions have revenue"]
            )
        )
        print(
            log_formatter.export_output(
                validation_results["All session ids are distinct"]
            )
        )
        print(
            log_formatter.export_output(
                validation_results["TransactionID is in the correct format"]
            )
        )

        # ------ Setting up the "Events" data node ------
        bq_events = pipe.nodes.create_node(
            "Events",
            DataNodeBigQuery,
            project_id="dp6-estudos",
            dataset_id="test_pipeline_penguin",
            table_id="vw_events",
        )

        bq_events.insert_premise(
            "Check if events are ecommerce actions",
            DataPremiseSQLCheckInArray,
            column="eventCategory",
            array=["Enhanced Ecommerce"],
        )

        # Running premise and printing the results
        validation_results = bq_events.run_premises()

        print(
            log_formatter.export_output(
                validation_results["Check if events are ecommerce actions"]
            )
        )
        assert False

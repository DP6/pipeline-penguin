from pipeline_penguin.data_premise.sql.check_arithmetic import (
    DataPremiseSQLCheckArithmeticOperationEqualsResult,
)
import pytest
import json

# Imports for mocking
import pandas as pd
from google.oauth2.service_account import Credentials
from os import path

# Pipeline Pengguin
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
from pipeline_penguin.data_premise.sql.check_between import (
    DataPremiseSQLCheckValuesAreBetween,
)
from pipeline_penguin.data_premise.sql.check_comparison import (
    DataPremiseSQLCheckLogicalComparisonWithValue,
)


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
    def mock_pandas_read_gbq(query, credentials, max_results, project_id):
        return pd.DataFrame([[0, 0]], columns=["total", "result"])

    monkeypatch.setattr(pd, "read_gbq", mock_pandas_read_gbq)


class TestFullExecution:
    def test_setup_PipelinePenguin_instance(
        self, _mock_from_service_account_file, _mock_is_file, _mock_pandas_read_gbq
    ):
        pp = PipelinePenguin()

        assert isinstance(pp, PipelinePenguin)

        gcp_service_acc = "./service_acc.json"
        bq_conn = ConnectorSQLBigQuery(gcp_service_acc)
        pp.connectors.define_default(bq_conn)

        assert pp.connectors.get_default(ConnectorSQLBigQuery) == bq_conn

    def test_default_execution(
        self,
        _mock_from_service_account_file,
        _mock_is_file,
        _mock_pandas_read_gbq,
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

        all_results = bq_conversions.run_premises()

        p1_results = all_results.outputs.get("All transactions have revenue")
        assert p1_results.pass_validation
        assert p1_results.data_node == bq_conversions

        p2_results = all_results.outputs.get("All session ids are distinct")
        assert p2_results.pass_validation
        assert p2_results.data_node == bq_conversions

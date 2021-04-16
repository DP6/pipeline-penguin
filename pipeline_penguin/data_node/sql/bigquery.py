"""This module provides the DataNodeBigQuery constructor."""

from pipeline_penguin.core.data_node import DataNode, NodeType
from pipeline_penguin.core.data_premise import PremiseType


class DataNodeBigQuery(DataNode):
    """Constructor for the DataNodeBigQuery.

    Args:
        name: Name for this datanode
        project_id: GCP project where the data is stored
        dataset_id: BigQuery's dataset where the data is stored
        table_id: BigQuery's table containing the data
        service_account_json: Path for authentication file for accessing the GCP project
    Attributes:
        premises: Dictionary holding every data_premise inserted
        supported_premise_types: Array of premise types allowed to be
            inserted on the data_node
        source: Type of data source, it is always "BigQuery"
    """

    def __init__(self, name, project_id, dataset_id, table_id, service_account_json):
        """Initialize the constructor."""
        super().__init__(name, NodeType.BIG_QUERY)
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.table_id = table_id
        self.service_account_json = service_account_json
        self.supported_premise_types = [PremiseType.SQL]

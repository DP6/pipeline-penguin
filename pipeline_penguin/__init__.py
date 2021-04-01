"""pipeline_penguin/__init__.py"""

from .data_nodes.data_node_bigquery import DataNodeBigQuery
from .pipeline_penguin import PipelinePenguin


class NodeType:
    BIG_QUERY = DataNodeBigQuery

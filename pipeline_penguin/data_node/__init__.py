"""Main data_node package, contains high-level data_nodes for specific data sources (BigQuery,
Cloud Storage, Google Sheets, Redshift, etc) and the node_manager module.

This package provides`DataNode` constructors, which are data structures responsible for
abstracting a single data source on an existing data pipeline. It also provides the `NodeManager`
class, used for creating, deleting and managing multiple DataNodes.

Location: pipeline_penguin/data_node/
"""
from .node_manager import NodeManager

__all__ = ["NodeManager", "sql"]

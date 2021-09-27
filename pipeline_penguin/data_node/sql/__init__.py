"""Main data_node package, contains high-level data_nodes for SQL-based data sources (BigQuery,
MySQL, etc).

This package provides SQL-based `DataNode` constructors, which are data structures responsible for
abstracting a single data source on an existing data pipeline.

Location: pipeline_penguin/data_node/sql/
"""
__all__ = ["bigquery"]

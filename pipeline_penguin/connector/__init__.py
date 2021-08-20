"""Main connector package, contains high-level connectors and the `connector_manager` module.

This package stores high-level connector modules used for communication with the data sources
upon execution of the data premises. It also contains the `ConnectorManager` object, responsible
for defining and managing the default connectors.

Location: pipeline_penguin/connector/
"""
from .connector_manager import ConnectorManager

__all__ = ["ConnectorManager", "sql"]

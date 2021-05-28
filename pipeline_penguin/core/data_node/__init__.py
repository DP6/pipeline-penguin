"""Core data_node package, contains abstract `DataNode` classes.

This package stores abstract classes used for construction of `DataNode` objects, which are
responsible for representing a single data source on an existing data pipeline.

The classes provided by its modules should not be instantiated directly, instead they are designed
to be inherited by other classes for more specific data sources.

It also stores constant variables for identifying NodeTypes across other modules used by
pipeline_penguin.

Location: pipeline_penguin/core/data_node/

"""

from .data_node import DataNode


class NodeType:
    """Constants for identifying specific data sources across other pipeline_penguin modules"""

    BIG_QUERY = "BigQuery"

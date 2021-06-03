"""Core connector package, contains the abstract `Connector` classes.

This package stores abstract classes used for construction of `Connector` objects, which are
responsible for communication with different databases or other sources of data.

The classes provided by its modules should not be instantiated directly, instead they are designed
to be inherited by other classes for more specific data sources.

Location: pipeline_penguin/core/connector/
"""
from .connector import Connector
from .sql import ConnectorSQL

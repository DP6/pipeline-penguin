"""Core data_premise package, contains abstract `DataPremise` parent constructors.

This package stores abstract classes used for construction of `DataPremise` objects, which
represent a validation to be executed on a given `DataNode`.

The classes provided by its modules should not be instantiated directly, instead they are designed
to be inherited by other classes for more specific data sources.

It also stores constant variables for identifying PremiseTypes across other modules used by
pipeline_penguin.

Location: pipeline_penguin/core/data_premise/
"""
from .data_premise import DataPremise


class PremiseType:
    """Constants for identifying specific data premises across other pipeline_penguin modules"""

    SQL = "SQL"

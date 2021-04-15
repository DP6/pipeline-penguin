"""This module provides the abstract DataNode constructor."""

import inspect
from typing import Callable, Dict, Type, Any, List

from pipeline_penguin.core.data_premise import DataPremise
from pipeline_penguin.exceptions import WrongTypeReference


class DataNode:
    """Base DataNode class.

    Args:
        name: Reference name of this DataNode
        source: Type of data source
    Attributes:
        premises: Dictionary holding every data_premise inserted
        supported_premise_types: Array of premise types allowed to be
            inserted on the data_node
    """

    def __init__(self, name: str, source: str):
        """Initialize the Data Node."""
        self.name = name
        self.source = source
        self.premises: Dict[str, Type[DataPremise]] = {}
        self.supported_premise_types = []

    @staticmethod
    def _is_data_premise_subclass(premise_factory: Any) -> bool:
        """Return whether the given premise factory is a subclass of DataPremise or not.

        Args:
            premise_factory: Any object to be validated.
        """
        return (
            premise_factory != DataPremise
            and inspect.isclass(premise_factory)
            and issubclass(premise_factory, DataPremise)
        )

    def insert_premise(
        self, name: str, premise_factory: Type[DataPremise], *args, **kwargs
    ) -> None:
        """Insert a premise on the DataNode.

        Args:
            name: name of the premise
            premise_factory: class constructor to be used in premise creation
            *args, **kwargs: Arguments passed down to the given constructor
        Raises:
            WrongTypeReference: If the given constructor is not a subclass of DataPremise
            WrongTypeReference: If the given constructor is of an unsupported type
        """
        if not self._is_data_premise_subclass(premise_factory):
            raise WrongTypeReference(
                "premise_factory param should be subclass of DataPremise"
            )

        premise = premise_factory(name, *args, **kwargs)

        if not premise.type in self.supported_premise_types:
            raise WrongTypeReference(
                f"{premise} should be of a supported premise type.\nSupported Types: {self.supported_premise_types}"
            )

        self.premises.update({premise.name: premise})

    def remove_premise(self, name: str) -> None:
        """Remove an previously inserted premise.

        Args:
            name: name of the premise to be removed
        """
        del self.premises[name]

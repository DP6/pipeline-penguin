"""This module provides the abstract DataNode constructor."""
import inspect
from typing import Dict, Type, Any

from pipeline_penguin.core.data_premise import DataPremise
from pipeline_penguin.exceptions import WrongTypeReference
from pipeline_penguin.core.connector.connector import Connector


class DataNode:
    """Base DataNode class.

    Args:
        name: Reference name of this DataNode
        source: Type of data source
    Attributes:
        premises: Dictionary holding every data_premise inserted
        supported_premise_types: Array of premise types allowed to be inserted on the data_node
        connectors: Custom data Connectors to be used while extracting data for this Node.
    """

    def __init__(self, name: str, source: str):
        """Initialize the Data Node."""
        self.name = name
        self.source = source
        self.premises: Dict[str, Type["DataPremise"]] = {}
        self.supported_premise_types = []
        self.connectors = {}

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
        premise = premise_factory(name, self, *args, **kwargs)

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

    def get_connector(self, premise_type: str) -> Connector:
        """Abstract method for retrieving the Connector to be used while querying data for
        this DataNode.
        If there's no corresponding Connector inside the internal `connectors` attribute, we look
        for one from the ConnectorManager.

        Args:
            premise_type (str): Type of Premise for identifying the Connector.

        Returns:
            Connector: Connector retrieved.
        """
        return

    def run_premises(self) -> Dict:
        """Run every DataPremise validation for this DataNode, printing the results and saving them
        on a Dictionary.

        Returns:
            Dict: Conolidation of all validations executed.
        """
        results = {}

        for name, premise in self.premises.items():
            passed = premise.validate()
            results[name] = passed
            print(f"{name}: {'passed' if passed else 'failed'}")

        return results

    def to_serializeble_dict(self) -> dict:
        return {}
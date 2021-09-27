"""Core data_node module, contains the abstract `DataNode` class.

DataNode objects represent a single data source on an existing data pipeline where we execute
the desired validations.

This modules provides the `DataNode` abstract constructor. Designed to be inherited by other
classes for building more specific data sources, it must not be instantiated directly.

Location: pipeline_penguin/core/data_node/

Example usage:

```python
class DataNodeBigQuery(DataNode):
    def __init__(self):
        # ...
        # Code for initializing the BigQuery Connector.
        # ...
        super().__init__(NodeType.BIG_QUERY)

    def to_serializeble_dict(self):
        # ...
        # Code for summarizing the DataNode as a dictionary
        # ...
        return {}
```
"""
from pipeline_penguin.core.connector.connector import Connector
from pipeline_penguin.core.premise_output.output_manager import OutputManager
from pipeline_penguin.core.data_premise import DataPremise
from pipeline_penguin.exceptions import WrongTypeReference

import inspect
from typing import Dict, Type, Any


class DataNode:
    """Abstract parent constructor for building other DataNode classes.

    DataNode objects represent a single data source on an existing data pipeline where we execute
    the desired validations.

    Args:
        name: Reference name of this DataNode
        source: Type of data source
    Attributes:
        premises: Dictionary storing data_premises registered on this DataNode.
        supported_premise_types: Array of premise types allowed to be registered on the DataNode.
        connectors: Custom data Connectors to be used while extracting data for this specific
                    DataNode (In contrast to the Default Connectors used by the ConnectorManager)
    """

    def __init__(self, name: str, source: str):
        self.name = name
        self.source = source
        self.premises: Dict[str, Type[DataPremise]] = {}
        self.supported_premise_types = []
        self.connectors = {}

    @staticmethod
    def _is_data_premise_subclass(premise_factory: Any) -> bool:
        """Return whether the given constructor class is DataPremise subclass or not.

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
        """Insert a Data Premise on the DataNode.

        Args:
            name: name of the premise
            premise_factory: The __class constructor__ to be used in premise creation. Not to be
                             mistaken by an previously instantiated DataPremise class
            *args, **kwargs: Arguments passed down to the given constructor
        Raises:
            WrongTypeReference: If the given constructor is not a subclass of DataPremise or is of
                                an unsupported type
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
        """Remove an previously inserted DataPremise given its name.

        Args:
            name: name of the premise to be removed
        """
        del self.premises[name]

    def get_connector(self, premise_type: str) -> Connector:
        """Abstract method for retrieving the Connector to be used while querying data from this
        DataNode.

        If there's no corresponding Connector inside the internal `connectors` attribute, we must
        look for one from the ConnectorManager.

        Args:
            premise_type (str): Type of Premise for identifying the Connector.

        Returns:
            Connector: Connector retrieved.
        """
        return

    def run_premises(self) -> OutputManager:
        """Run every DataPremise validation for this DataNode, printing their validation status and
        saving them on a Dictionary.

        Returns:
            A `dictionary` object consolidating all validations executed.
        """
        output_mgr = OutputManager()

        for premise_name, premise in self.premises.items():
            premise_output = premise.validate()
            output_mgr.outputs.update({premise_name: premise_output})
            print(
                f"{self.name} - {premise_name}: \
                  {'Passed' if premise_output.pass_validation else 'Failed'}"
            )

        return output_mgr

    def to_serializeble_dict(self) -> Dict:
        """Method for constructing a dictionary representation of the current DataNode using
        only built-in data types.

        Returns:
            A `dictionary` object containing the DataNode representation.
        """
        return {}

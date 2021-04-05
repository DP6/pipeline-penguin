import inspect
from typing import Callable, Dict, Type, Any

from pipeline_penguin.core.data_premise import DataPremise
from pipeline_penguin.exceptions import WrongTypeReference


PremiseFactory = Callable[..., Type["PremiseCreatorClass"]]


class DataNode:
    def __init__(self, name: str, source: str):
        """
        Constructor for the DataNode.
        Must check if the source is a valid, supported data source.
        """
        self.name = name
        self.source = source
        self.premises: Dict[str, Type[PremiseFactory]] = {}

    @staticmethod
    def _is_data_premise_subclass(premise: Any) -> bool:
        return (
            premise != DataPremise
            and inspect.isclass(premise)
            and issubclass(premise, DataPremise)
        )

    def insert_premise(self, name: str, premise_factory: PremiseFactory) -> None:
        """
        Receives the name of premise and an DataPremise class.
        Must insert the DataPremise instance on the premises dictionary using name as key.
        Must overwrite the DataPremise.name for the name passed on parameter.
        """

        if not self._is_data_premise_subclass(premise_factory):
            raise WrongTypeReference(
                "premise_factory param should be subclass of DataPremise"
            )

        premise = premise_factory(name=name)
        self.premises.update({name: premise})

    def remove_premise(self, name: str) -> None:
        """
        Receives the name of premise.
        Must remove the DataPremise instance on the premises dictionary.
        """

        if not isinstance(name, str):
            raise WrongTypeReference(
                "Name of premise should be passed as string argument"
            )

        del self.premises[name]

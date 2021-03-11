from typing import Callable, Dict, Type

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

    def insert_premise(self, name: str, premise_factory: PremiseFactory) -> None:
        """
        Receives the name of premise and an DataPremise class.
        Must insert the DataPremise instance on the premises dictionary using name as key.
        Must overwrite the DataPremise.name for the name passed on parameter.
        """
        premise = premise_factory(node=self, name=name)
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

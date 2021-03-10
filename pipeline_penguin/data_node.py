from typing import Callable, Dict, Type, Union

from pipeline_penguin.data_premise import DataPremise
from pipeline_penguin.exceptions import WrongTypeReference

PremiseFactory = Callable[..., Type[DataPremise]]


class DataNode:
    def __init__(self, name: str, source: str):
        """
        Constructor for the DataNode.
        Must check if the source is a valid, supported data source.
        """
        self.name = name
        self.source = source
        self.premises: Dict[PremiseFactory()] = {}

    @staticmethod
    def _is_data_premise_instance(premise) -> bool:
        return isinstance(premise, DataPremise)

    def insert_premise(self, name: str, premise_factory: PremiseFactory):
        """
        Receives the name of premise and an DataPremise class.
        Must insert the DataPremise instance on the premises dictionary using name as key.
        Must overwrite the DataPremise.name for the name passed on parameter.
        Returns None
        """
        premise = premise_factory(node=self, name=name)
        self.premises.update({name: premise})

    def remove_premise(self, premise: Union[str, Type[DataPremise]]):
        """
        Receives an String or an DataPremise instance.
        Must remove the DataPremise instance on the premises dictionary.
        Returns None
        """

        if self._is_data_premise_instance(premise):
            # TODO to remove DataPremise instance insert_premise should return the instance
            pass
        elif type(premise) == str:
            del self.premises[premise]
        else:
            raise WrongTypeReference(
                "String or DataPremise instance should be passed in premise argument"
            )

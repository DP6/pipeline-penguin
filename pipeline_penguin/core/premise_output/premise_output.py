"""This module provides the abstract PremiseOutput constructor."""
import pandas as pd
from typing import Dict


class PremiseOutput:
    """A PremiseOutput represents the results of a DataPremise Validation.

    Args:
        data_premise: The DataPremise that generated this output.
        data_node: The DataNode that the DataPremise ran on.
        column: Column name.
        pass_validation: Indicates if it passed the DataPremise rules without any exceptions or not.
        failed_count: Count the number of exceptions in the DataPremise validation.
        failed_values: A Pandas DataFrame carrying the exception values found.
    """

    def __init__(
        self,
        data_premise: "DataPremise",
        data_node: "DataNode",
        column: str,
        pass_validation: bool,
        failed_count: int,
        failed_values: pd.DataFrame(),
    ):
        """Initialization of the DataPremise."""
        self.data_premise = data_premise
        self.data_node = data_node
        self.column = column
        self.pass_validation = pass_validation
        self.failed_count = failed_count
        self.failed_values = failed_values

    def to_serializeble_dict(self) -> Dict:
        results = {
            "pass_validation": self.pass_validation,
            "failed_values": self.failed_values,
            "failed_count": self.failed_count,
        }
        results.update({"data_premise": self.data_premise.to_serializeble_dict()})
        results.update({"data_node": self.data_node.to_serializeble_dict()})

        return results

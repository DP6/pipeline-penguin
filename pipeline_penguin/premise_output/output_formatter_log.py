r"""Contains the `OutputFormatterLog`constructor, used format the premise results into a human-readable message

Location: pipeline_penguin/core/premise_output/

Example usage:

```python
formatter = OutputFormatterLog()
formatter.format(premise_output)
```
"""

from pipeline_penguin.core.premise_output.output_formatter import OutputFormatter
from pipeline_penguin.core.premise_output.premise_output import PremiseOutput
import json
import numpy as np
import decimal


class NpEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.integer):
            return int(obj)
        elif isinstance(obj, np.floating):
            return float(obj)
        elif isinstance(obj, decimal.Decimal):
            return float(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        elif isinstance(obj, np.bool_):
            return bool(obj)
        else:
            return super(NpEncoder, self).default(obj)


class OutputFormatterLog(OutputFormatter):
    """Contains the `OutputFormatterLog`constructor, used format the premise results into
    a human-readable message
    """

    def format(self, premise_output: PremiseOutput) -> str:
        """Construct a human-readable message based on the results of a premise validation.

        Args:
            premise_output: PremiseOutput object to be formatted
        Returns:
            str: Human-readable message with PremiseOutput's data
        """
        data_premise = premise_output.data_premise
        output_data = premise_output.to_serializeble_dict()

        json_data = json.dumps(output_data, indent=4, sort_keys=True, cls=NpEncoder)

        msg = f"Results of {data_premise.name} validation:\n{json_data}"
        return msg

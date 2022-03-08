r"""Contains the `OutputFormatterJSON`constructor, used format the premise results into a JavaScript Object Notation

Location: pipeline_penguin/core/premise_output/

Example usage:

```python
formatter = OutputFormatterJSON()
formatter.format(premise_output)
```
"""

from pipeline_penguin.core.premise_output.output_formatter import OutputFormatter
from pipeline_penguin.core.premise_output.premise_output import PremiseOutput
import json
from typing import Dict

class OutputFormatterJSON(OutputFormatter):
    """Contains the `OutputFormatterJSON`constructor, used format the premise 
    results into a JavaScript Object Notation
    """

    def format(self, premise_output: PremiseOutput) -> str:
        """Abstract method for transforming the given PremiseOutput into the desired format."""
        
        premise_output_result = premise_output.to_serializeble_dict() 
        json_premise_output_result = json.dumps(premise_output_result)

        return json_premise_output_result

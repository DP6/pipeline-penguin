r"""Core premise_output module, contains the  abstract `OutputFormatter` class.

OutputFormatters are used to export the results of previously executed validations into a specific
format (JSON, csv, xml, etc).

The class provided by this module is a abstract constructor, it is designed to be used as a base for
building other types of Formatters and should not be be instatiated directly.

Location: pipeline_penguin/core/premise_output/

Example usage:

```python
class OutputFormatterLog(OutputFormatter):
    def export_output(self, premise_output: "PremiseOutput") -> str:
        data_premise = premise_output.data_premise
        output_data = premise_output.to_serializeble_dict()

        json_data = json.dumps(output_data, indent=4, sort_keys=True)

        msg = f"Results of {data_premise.name} validation:\n{json_data}"
        return msg
```
"""


from typing import Any


class OutputFormatter:
    """Module used for exporting the results of a previously executed validation into a specific
    data format (JSON, csv, xml, etc).
    """

    def export_output(
        self, premise_output: "pipeline_penguin.core.premise_output.PremiseOutput", *args, **kwargs
    ) -> Any:
        """Abstract method for transforming the given PremiseOutput into the desired format."""
        pass

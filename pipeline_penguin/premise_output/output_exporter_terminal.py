r"""Contains the `OutputExpoterTerminal` constructor, used to send send the formatter's results to the console.

Location: pipeline_penguin/core/premise_output/

Example usage:

```python

formatter = OutputFormatterLog()
exporter = OutputExporterTerminal()

premise_output = bq_conversions_results.outputs.get("My Data Premise")
exporter.export_output(formatter.format(premise_output))
```
"""
from pipeline_penguin.core.premise_output.output_exporter import OutputExporter


## Formatar e exportar outputs 
class OutputExporterTerminal(OutputExporter):
    """Contains the `OutputExporterTerminal` constructor,used to send send the formatter's results to the console."""

    def export_output(self, content: str):
        """Sends a message to the console

        Args:
            content (str): message to be printed out
        """
        print(content)

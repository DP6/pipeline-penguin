r"""Core premise_output module, contains the  abstract `OutputExporter` class.

OutputExporter are used to deliver results from a executed validation to some destination (GCP/AWS Bucket, FTP server, API endpoint, etc)

The class provided by this module is a abstract constructor, it is designed to be used as a base for
building other types of Exporters and should not be be instantiated directly.

Location: pipeline_penguin/core/premise_output/

Example usage:

```python
TODO: Add an example when ready
```
"""


class OutputExporter:
    """Module used for exporting the results of a previously executed validation to a given destination (Cloud Bucket, FTP, HTTPS endpoint, etc)
    """

    def export_outputs(self, validation_results, *args, **kwargs):
        """Stub method for sending the given results to the destination.

        Args:
            validation_results (Any): The results of a OutputFormatter.format call over an PremiseOutput object
        """
        pass

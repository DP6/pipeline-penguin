r"""Contains the `OutputExporterHTTP` constructor,  used to send data_premises results to an HTTP endpoint.

It sends a request with parameters:

Location: pipeline_penguin/core/premise_output/

Example usage:

```python
exporter = OutputExporterHTTP()
exporter.export_output(
    "localhost:8080",
    method="POST",
    body={"payload": {"my-data": 123}}
)
```
"""
from pipeline_penguin.core.premise_output.output_exporter import OutputExporter
from google.oauth2.service_account import Credentials

class OutputExporterHTTP(OutputExporter):
    """Contains the `OutputExporterHTTP` constructor, used to send data_premises results to an HTTP endpoint"""

    def export_output(
        self,
        url: str,
        method: str="POST",
        body: dict={},
        credentials: Credentials=None,
    ):
        """Sends a request to a given HTTP destination

        Args:
            url (str): HTTP(s) address for sending the request
            method (str, optional): Method to use (GET, POST, PUT, etc). Defaults to "POST".
            body (dict, optional): Body of the request. Defaults to {}.
            credentials (Credentials, optional): Credentials to use for an authorized google request. Defaults to None.
        """
        pass

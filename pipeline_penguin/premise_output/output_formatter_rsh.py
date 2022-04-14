r"""Contains the `OutputFormatterRSH` constructor, used to convert a premise_output into the json payload needed for an Raft Suite Hub endpoint.

Location: pipeline_penguin/core/premise_output/

Example usage:

```python
formatter = OutputFormatterRSH()
formatter.format(premise_output)
```
"""


from pipeline_penguin.core.premise_output.output_formatter import OutputFormatter
from pipeline_penguin.core.premise_output.premise_output import PremiseOutput
from google.oauth2.service_account import Credentials

import google.auth
from os import path


class OutputFormatterRSH(OutputFormatter):
    """Contains the `OutputFormatterRSH` constructor, used to sent data_premises results to an RSH cloud function."""

    def format(
        self,
        premise_output: PremiseOutput,
        project: str,
        spec: str,
        deploy: str,
        code: str,
        description: str,
    ) -> str:
        """Converts a premise_output into the json payload needed for an Raft Suite Hub endpoint.

        List of parameters with data contained by the resulting json::

        | Parameters                   | Type | Example                               | Description                                             |
        |------------------------------|------|---------------------------------------|---------------------------------------------------------|
        | body.project                 | str  | Project A                             | Name of the project                                     |
        | body.module                  | str  | pipeline-penguin                      | Name of the module                                      |
        | body.spec                    | str  | analytics_to_bigquery                 | Name of the pipeline                                    |
        | body.deploy                  | str  | 0.2                                   | Version of the Pipeline Penguin                         |
        | body.code                    | str  | 00-00                                 | Type of validation code and its status                  |
        | body.description             | str  | Checking if there is null on column X | Description of the validation done                      |
        | body.payload.data_premise    | str  | check_null                            | Name of the DataPremise                                 |
        | body.payload.data_node       | str  | data_a                                | Name of the DataNode                                    |
        | body.payload.column          | str  | product_id                            | Name of the column                                      |
        | body.payload.pass_validation | bool | true                                  | Indicated whether the data passed the validation or not |
        | body.payload.failed_count    | int  | 5                                     | Count of rows that failed the validation                |
        | body.payload.failed_values   | dict |                                       | Dict of examples that failed the validation             |


        Args:
            premise_output (PremiseOutput): PremiseOutput object to be formatted
            url (str): URL for the RSH cloud function
            project (str): Project name running the validations
            spec (str): Name of the pipeline
            deploy (str): Version of the Pipeline Penguin
            code (str): Type of validation code and its status
            description (str): Description of the validation done
        Returns:
            str: stringfied json with the premise_output's data.
        """
        payload = {
            "project": project,
            "module": "pipeline-penguin",
            "spec": spec,
            "deploy": deploy,
            "code": code,
            "description": description,
            "payload": {
                "data_premise": premise_output.data_premise,
                "data_node": premise_output.data_node,
                "column": premise_output.column,
                "pass_validation": premise_output.pass_validation,
                "failed_count": premise_output.failed_count,
                "failed_values": premise_output.failed_values.to_json(),
            },
        }

        return payload

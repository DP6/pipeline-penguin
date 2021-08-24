r"""Contains the `OutputFormatterRSH` constructor, which parses a PremiseOutput into a json object to be sent to a Cloud Function

The resulting object is composed of the following parameters:

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

Location: pipeline_penguin/core/premise_output/

Example usage:

```python
formatter = OutputFormatterRSH()
body = formatter.export_output(premise_output)
```
"""


from pipeline_penguin.core.premise_output.output_formatter import OutputFormatter
from google.oauth2.service_account import Credentials
from typing import Dict
import json


class OutputFormatterRSH(OutputFormatter):
    """Contains the `OutputFormatterRSH` constructor, used to sent data_premises results to an RSH cloud function."""

    def export_output(
        self,
        premise_output: "PremiseOutput",
        project: str,
        spec: str,
        deploy: str,
        code: str,
        description: str,
    ) -> Dict:
        """Structures a PremiseOutput into a json format defined by the Raft Suite Hub.
        Reference: https://dp6.github.io/raft-suite-hub/

        Args:
            premise_output: PremiseOutput object to be formatted
            url: URL for the RSH cloud function
            project: Project name running the validations
            spec:
            deploy:
            code:
            description:
            credentials_path: Path to service_account for authentication
        Returns:
            Dict : json data to be sent on the "body" of a POST request
        """
        body = {
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

        json.dumps(body)  # Checking if the object is serializable

        return body

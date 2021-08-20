r"""Contains the `OutputFormatterRSH` constructor, used to sent data_premises results to an RSH cloud function.

It sends a request with parameters:

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
formatter.export_output(premise_output)
```
"""


from pipeline_penguin.core.premise_output.output_formatter import OutputFormatter
from pipeline_penguin.core.premise_output.premise_output import PremiseOutput
from google.oauth2.service_account import Credentials

import google.auth
from os import path


def make_authorized_post_request(service_url, body, credentials):
    """
    make_authorized_get_request makes a GET request to the specified HTTP endpoint
    in service_url (must be a complete URL) by authenticating with the
    ID token obtained from the google-auth client library.
    """
    auth_sess = google.auth.transport.requests.AuthorizedSession(credentials)
    resp = auth_sess.post(service_url, json=body)
    return resp


class OutputFormatterRSH(OutputFormatter):
    """Contains the `OutputFormatterRSH` constructor, used to sent data_premises results to an RSH cloud function."""

    def export_output(
        self,
        premise_output: PremiseOutput,
        url: str,
        project: str,
        spec: str,
        deploy: str,
        code: str,
        description: str,
        credentials_path: str = "default",
    ) -> str:
        """Send a request to structure defined by Raft Suite Hub.

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
            None
        """

        if credentials_path == "default":
            print("Using google.auth.default credentials")
            credentials, project_id = google.auth.default()
        elif path.isfile(credentials_path):
            credentials = Credentials.from_service_account_file(credentials_path)

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

        return make_authorized_post_request(url, body, credentials)

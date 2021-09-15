r"""Contains the `OutputFormatterHTTPCloudFunction` constructor, used to send requests to the HTTP endpoint of a Cloud Function

Location: pipeline_penguin/premise_output/

Example usage:

```python
exporter = OutputFormatterHTTPCloudFunction()
exporter.export_output(
    ur="http://my-cloud-function-endpoint",
    headers={},
    body={"data": "Some cool data"}},
    credentials_path="path/to/service_account.json"
)
```
"""


from pipeline_penguin.core.premise_output.output_exporter import OutputExporter

from google.oauth2.service_account import Credentials
from google import auth
from typing import Dict

import requests
from build.lib.pipeline_penguin.core.premise_output.output_manager import premise_output


def make_authorized_post_request(
    service_url: str, headers: Dict, body: Dict, credentials: Credentials, **kwargs
) -> requests.Response:
    """Makes an authenticated POST request to the given HTTP endpoint.

    Args:
        service_url (str): HTTP endpoint for a Cloud Function
        headers (Dict): request headers
        body (Dict): request body
        credentials (Credentials): credentials for authentication

    Returns:
        Response: Object with the response from the Cloud Function
    """

    auth_sess = auth.transports.requests.AuthorizedSession(credentials)
    return auth_sess.post(service_url, headers=headers, data=body, **kwargs)


class OutputExporterCloudFunctionHttp(OutputExporter):
    def __init__(
        self,
        url: str,
        headers: Dict = {},
        credentials_path: str = None,
        req_args: Dict = {},
    ) -> None:
        """[summary]

        Args:
            url (str): Endpoint
            headers (Dict, optional): Request headers. Defaults to {}.
            credentials_path (str, optional): Path to service account for authentication. Defaults to None.
            req_args (Dict, optional): Additional arguments to use on the request. Defaults to {}.
        """
        super().__init__()
        self.url = url
        self.headers = headers
        self.credentials_path = credentials_path
        self.req_args = req_args

    def export(self, body) -> str:
        """Fires the request to a given Google Cloud Function Endpoint. May be authenticated or not.

        Args:
            premise_output: PremiseOutput object to be formatted
            body (Dict): request body
            **kwargs: Other arguments for the request
        Returns:
            Response: Object with the response from the Cloud Function
        """

        if self.credentials_path is None:
            print("Firing unauthenticated request")
            response = requests.post(
                self.url, headers=self.headers, data=body, **self.req_args
            )
        elif self.credentials_path == "default":
            print("Using google.auth.default credentials")
            credentials, project_id = auth.default()
        else:
            credentials = Credentials.from_service_account_file(self.credentials_path)

        print("Firing authenticated request")
        response = make_authorized_post_request(
            self.url, self.headers, body, credentials, **self.req_args
        )
        result = "success" if 200 <= response.status_code < 300 else "failure"

        return {"result": result, "contents": response}

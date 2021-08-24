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


class OutputFormatterHTTPCloudFunction(OutputExporter):
    """Sends data to a HTTP Cloud Function."""

    def export_output(
        self,
        url: str,
        headers: Dict = {},
        body: Dict = {},
        credentials_path: str = None,
        **kwargs
    ) -> str:
        """Fires the request to a given Cloud Function.

        Args:
            premise_output: PremiseOutput object to be formatted
            url: URL for the RSH cloud function
            headers (Dict): request headers
            body (Dict): request body
            credentials_path: Path to service_account for authentication
            **kwargs: Other arguments for the request
        Returns:
            Response: Object with the response from the Cloud Function
        """

        if credentials_path is None:
            print("Firing unauthenticated request")
            return requests.post(url, headers=headers, data=body, **kwargs)
        elif credentials_path == "default":
            print("Using google.auth.default credentials")
            credentials, project_id = auth.default()
        else:
            credentials = Credentials.from_service_account_file(credentials_path)
        print("Firing authenticated request")
        return make_authorized_post_request(url, headers, body, credentials)

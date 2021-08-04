r"""Contains the `OutputFormatterRHS` constructor, used to sent data_premises results to an RHS cloud function.

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
formatter = OutputFormatterRHS()
formatter.export_output(premise_output)
```
"""

from pipeline_penguin.core.premise_output.output_formatter import OutputFormatter


class OutputFormatterRHS(OutputFormatter):
    """Contains the `OutputFormatterRHS` constructor, used to sent data_premises results to an RHS cloud function."""

    def export_output(
        self, premise_output: "PremiseOutput", url: str, project: str
    ) -> str:
        """Send a request to structure defined by Raft Suit Hub.

        Args:
            premise_output: PremiseOutput object to be formatted
            url: URL for the RHS cloud function
            project: Project name running the validations
        Returns:
            None
        """
        pass

from pipeline_penguin.core.data_premise.sql import DataPremiseSQL
from pipeline_penguin.core.premise_output.premise_output import PremiseOutput
from pipeline_penguin.data_node.sql.bigquery import DataNodeBigQuery

class CheckIsNull(DataPremiseSQL):
    def __init__(
        self,
        name: str,
        data_node: DataNodeBigQuery,
        column: str
    ):
        super().__init__(name, data_node, column)
        self.query_template = """
            WITH t AS(
                SELECT 
                    event_id,
                    session_id,
                    event_type,
                    page_url,
                    LAG(event_type) OVER (PARTITION BY session_id ORDER BY event_timestamp) AS previous_event_type,
                    LAG(page_url) OVER (PARTITION BY session_id ORDER BY event_timestamp) AS previous_page_url
                FROM `{project}.{dataset}.{table}`
                ORDER BY session_id
            )

            SELECT event_id AS result FROM t WHERE {column} IS NULL
        """

    def query_args(self):
        """Method for returning the arguments to be passed on the query template of this
        validation.

        Returns:
            A `dictionary` with the query parameters.
        """
        return {
            "project": self.data_node.project_id,
            "dataset": self.data_node.dataset_id,
            "table": self.data_node.table_id,
            "column": self.column
        }

    def validate(self) -> PremiseOutput:
        """Method for executing the validation over the DataNode.

        Returns:
            PremiseOutput: Object storeing the results for this validation.
        """

        query = self.query_template.format(**self.query_args())
        connector = self.data_node.get_connector(self.type)
        data_frame = connector.run(query)

        failed_count = len(data_frame["result"])
        passed = failed_count == 0

        output = PremiseOutput(
            self, self.data_node, self.column, passed, failed_count, data_frame
        )
        return output


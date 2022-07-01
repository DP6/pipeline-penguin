from pipeline_penguin.core.premise_output.output_formatter import OutputFormatter
from pipeline_penguin.core.premise_output.premise_output import PremiseOutput
from datetime import datetime
import json

class NpEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.integer):
            return int(obj)
        elif isinstance(obj, np.floating):
            return float(obj)
        elif isinstance(obj, decimal.Decimal):
            return float(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        elif isinstance(obj, np.bool_):
            return bool(obj)
        else:
            return super(NpEncoder, self).default(obj)


class OutputFormatterBigQuery(OutputFormatter):
    """Contains the `OutputFormatterBigQuery`constructor, used to save DataPremise results in 
    Big Query table."""

    def export_output(self, premise_output: PremiseOutput, project_id: str, dataset_id: str, table_id: str) -> str:
        """Construct a human-readable message based on the results of a premise validation.
        Args:
            premise_output: PremiseOutput object to be formatted
            project_id: GCP project id
            dataset_id: GCP Big Query dataset name
            table_id: GCP Big Query table name
        Returns:
            str: Human-readable message with PremiseOutput's data
        """

        data_premise = premise_output.data_premise
        output_data = premise_output.to_serializeble_dict()

        json_data = json.dumps(output_data, indent=4, sort_keys=True, cls=NpEncoder)

        msg = f"Results of {data_premise.name} validation:\n{json_data}"

        validate_df = premise_output.failed_values.copy()
        validate_df['validate_timestamp'] = datetime.now()

        connector = premise_output.data_node.get_connector('sql')
        validate_df.to_gbq(
            destination_table='{}.{}'.format(dataset_id, table_id), 
            project_id=project_id,
            credentials=connector.credentials,
            if_exists='append',
            table_schema=[
                {'name': 'results', 'type': 'STRING'},
                {'name': 'validate_timestamp', 'type': 'DATETIME'}
            ],
            progress_bar=False
        )
        
        return msg
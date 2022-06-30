from pipeline_penguin import PipelinePenguin
from pipeline_penguin.connector.sql.bigquery import ConnectorSQLBigQuery
from pipeline_penguin.data_node.sql.bigquery import DataNodeBigQuery
from pipeline_penguin.premise_output.output_formatter_log import OutputFormatterLog
from output_formatter_bq import OutputFormatterBigQuery
from CheckIsNull import CheckIsNull

def pipeline_penguin_Cf(request):
    """Responds to any HTTP request.
    Args:
        request (flask.Request): HTTP request object.
    Returns:
        The response text or any set of values that can be turned into a
        Response object using
        `make_response <http://flask.pocoo.org/docs/1.0/api/#flask.Flask.make_response>`.
    """

    PROJECT_ID = 'PROJECT_ID'
    DATASET_ID = 'DATASET_ID'
    TABLE_ID = 'TABLE_ID'
    TABLE_ID_RESULT = 'TABLE_ID_RESULT'
    
    pp = PipelinePenguin()

    bq_connector = ConnectorSQLBigQuery()
    pp.connectors.define_default(bq_connector)

    node = pp.nodes.create_node('Node Name', DataNodeBigQuery, project_id=PROJECT_ID, dataset_id=DATASET_ID, table_id=TABLE_ID)

    node.insert_premise('Check Is Null', CheckIsNull, "previous_event_type")

    log_formatter = OutputFormatterBigQuery()
    log_formatter.export_output(node.premises['Check Is Null'].validate(), project_id=PROJECT_ID, dataset_id=DATASET_ID, table_id=TABLE_ID_RESULT)

    request_json = request.get_json()
    if request.args and 'message' in request.args:
        return request.args.get('message')
    elif request_json and 'message' in request_json:
        return request_json['message']
    else:
        return f'Success!'

    

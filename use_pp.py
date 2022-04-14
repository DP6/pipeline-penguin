from pipeline_penguin import PipelinePenguin
from pipeline_penguin.connector.sql.bigquery  import ConnectorSQLBigQuery
from pipeline_penguin.data_node.sql.bigquery import DataNodeBigQuery
from pipeline_penguin.data_premise.sql.check_null import DataPremiseSQLCheckIsNull
from pipeline_penguin.core.node_relation.node_relation import NodeRelation


pp = PipelinePenguin()

bq_connector = ConnectorSQLBigQuery('autoline.json')
pp.connectors.define_default(bq_connector)

node = pp.nodes.create_node('autoline',DataNodeBigQuery ,project_id = 'autoline-250103',dataset_id = 'autoline',table_id='anuncios_2020')
node2 = pp.nodes.create_node('autoline',DataNodeBigQuery ,project_id = 'autoline-250103',dataset_id = 'autoline',table_id='anuncios')
print(pp.nodes.list_nodes())

node.insert_premise('Check Null', DataPremiseSQLCheckIsNull, "link")
node.add_relation(relation=node2,isDestination=True)
saida = node.get_relations()

print(saida)

vamover = NodeRelation.get_source(node)
vamover
rel = NodeRelation(source=node,destination=node2)
print(rel.get_source())
#print(saida)

#outputs = pp.nodes.run_premises()
#print(outputs.outputs['autoline'].outputs)




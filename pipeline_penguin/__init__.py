"""pipeline_penguin is a python library used for validating data pipelines. It works by building
an abstraction of the data sources in your pipeline (represented as DataNodes) and setting up
expected behaviours (represented as DataPremises) for each of their columns.

Location: /

"""

from pipeline_penguin.data_node.sql.bigquery import DataNodeBigQuery
from .pipeline_penguin import PipelinePenguin

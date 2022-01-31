## Description

Pipeline Penguin is a versatile python library for data quality.

## Documentation

- [Reference](https://dp6.github.io/pipeline-penguin/pipeline_penguin.html)

## Getting Started

### How to install

You can use PyPI Test to install the early develoment build by executing this command:

```
pip install pipeline-penguin
```

### Core Concepts

Before you start executing validations on you data, firstly you need to understand a few concepts:

#### Data Node
Any database your pipeline needs to process. It has a Node Type, that identifies what is the source of that data, like BigQuery. 

#### Data Premise
Any premise on a Data Node, for example, the column "name" on the database "employers" must not have a number on it. Data Premises also have a type called Premise Type. If an Premise Type is SQL, you cannot execute a validation on a Data Node with a Node Type that does not have a SQL engine to process that query.

#### Connector
The way you access an Data Node to check a Data Premise. 

#### Premise Output
Is the result of a Data Premise validation on a Data Node.

### Implementing a test

- Importing and instantiating PP

```python
from pipeline_penguin import PipelinePenguin
pp = PipelinePenguin()
```

- Defining the default connector

```python
bq_connector = ConnectorSQLBigQuery('/config/service_account.json')
pp.connectors.define_default(bq_connector)
```

- Creating a Data Node

```python
node = pp.nodes.create_node('Node Name', DataNodeBigQuery, project_id='example', dataset_id='example', table_id='example')
pp.nodes.list_nodes()
```

- Creating a Data Premise

```python
node.insert_premise('Premise Name', DataPremiseSQLCheckIsNull, "Column Name")
```

- Executing a validation

```python
pp.nodes.run_premises()
```

- Checking Logs

```python
log_formatter = OutputFormatterLog()
outputs.format_outputs(log_formatter)
```

### Implementing a custom Data Premise

- Implementing a new DataPremise class

```python

from pipeline_penguin.core.data_premise.sql import DataPremiseSQL
from pipeline_penguin.core.premise_output.premise_output import PremiseOutput
from pipeline_penguin.data_node.sql.bigquery import DataNodeBigQuery

class CheckBanana(DataPremiseSQL):
    def __init__(
        self,
        name: str,
        data_node: DataNodeBigQuery,
        column: str
    ):
        self.query_template = "SELECT * result FROM `{project}.{dataset}.{table}` WHERE LOWER({column}) = 'banana')"
        super().__init__(name, data_node, column)

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
```

- Testing a DataNode with a custom Data Premise

```python
from pipeline_penguin import PipelinePenguin
import CheckBanana

pp = PipelinePenguin()

bq_connector = ConnectorSQLBigQuery('/config/service_account.json')
pp.connectors.define_default(bq_connector)

node = pp.nodes.create_node('Node Name', DataNodeBigQuery, project_id='example', dataset_id='example', table_id='example')

node.insert_premise('Check Null', DataPremiseSQLCheckIsNull, "Column Name")
node.insert_premise('Check Contains Banana', CheckBanana, "Column Name")

log_formatter = OutputFormatterLog()
outputs.format_outputs(log_formatter)
```

## Collaborate
### Installation

```
pipenv install
```

### Tests

```
pipenv install --dev
```

Running tests

```
pipenv run test
```

### Style format
#### Running format

```
pipenv run format
```

#### Checking format

```
pipenv run format-check
```

### Developing documentation
#### Running local build
```
pipenv run docs
```

#### Bulding docs
```
pipenv run pipenv run build-docs
```

## Support or Contact
Having trouble with PP? Check out our [documentation](https://dp6.github.io/pipeline-penguin/pipeline_penguin.html) or contact support and we’ll help you sort it out.

DP6 Koopa-Troopa Team
e-mail: [koopas@dp6.com.br](koopas@dp6.com.br)
from pipeline_penguin.core.data_premises import DataPremise


class DataPremiseSQL(DataPremise):
    def __init__(self, name, type, column, query):
        """
        Constructor for the data_premise_sql.
        """
        super().__init__(name, type, column)
        self.query = query

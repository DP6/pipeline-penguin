from pipeline_penguin.core.data_premise_sql import DataPremiseSQL


class DataPremiseSQLCheckNull(DataPremiseSQL):
    def __init__(self, name, type, column):
        """
        Constructor for the data_premise_sql_check_null.
        """
        super().__init__(self, name, type, column, "")

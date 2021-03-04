class DataNode:
    def __init__(self, name, source):
        """
        Constructor for the DataNode.
        Must check if the source is a valid, supported data source.
        """
        self.name = name
        self.source = source
        self.premises = {}

    def insert_premise(name, premise):
        """
        Receives an String and an DataPremise instance.
        Must insert the DataPremise instance on the premises dictionary using name as key.
        Must overwrite the DataPremise.name for the name passed on parameter.
        Returns None
        """
        pass

    def remove_premise(premise):
        """
        Receives an String or an DataPremise instance.
        Must remove the DataPremise instance on the premises dictionary.
        Returns None
        """
        pass

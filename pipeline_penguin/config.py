"""This module provides an interface for exporting your pipeline's configuration into a JSON file or
importing a previously exported pipeline

Example usage:
```python
# Exporting
pp = PipelinePenguin()
Config.export_config(pp, "./my-pipeline.json")

# Importing
pp = Config.import_config("./my-pipeline.json")
```
"""



from . import PipelinePenguin

class Config:
    def import_config(path: str) -> PipelinePenguin:
        """Reads a given JSON configuration file and generates a PipelinePenguin instance

        Args:
            path (str): path for the JSON file

        Returns:
            PipelinePenguin: The code for running the pipeline's validation
        """
        pass

    def export_config(PipelinePenguin, path: str):
        """Generates a JSON configuration file with the context of a PipelinePenguin's instance

        Args:
            PipelinePenguin (_type_): The object instance where the pipeline has been configured.
            path (str): Path for saving the JSON file.
        """
        pass
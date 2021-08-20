r"""Core premise_output package, contains abstract `OutputFormatters` parent constructors.

The idea behind this package is to provided a variety of export options for the results of
each validation to be sent (i.e. printing on console, sending to a database, compiling a JSON file, 
etc)

The flow of data is composed by 3 main elements.

- __PremiseOutputs__. Which are generalized data strucutres for storing the validation results
before exporting to a given destination. Each PremiseOutput contains the results of a single
DataPremise execution.
- __OutputManager__. Stores PremiseOutputs for all DataPremises of every single DataNode in the
pipeline.
- __OutputFormatters__. Are designed format and export the PremiseOutput on accordingly to their
coded behavior. Many different formatters can be executed on the same PremiseOutputs.

The `OutputFormatter` class provided by this package should not be instantiated directly, instead
its designed to be inherited by other classes for building specific output behavior.

Location: pipeline_penguin/core/premise_output/
"""
from .output_formatter import OutputFormatter
from .premise_output import PremiseOutput
from .output_manager import OutputManager

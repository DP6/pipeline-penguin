r"""Core premise_output module, contains the  abstract `OutputFormatter` class.

OutputFormatters are used to generate the results received into a single specific
format (JSON, csv, xml, etc).

The class provided by this module is a abstract constructor, it is designed to be used as a base for
building other types of Formatters and should not be be instatiated directly.

Location: pipeline_penguin/core/premise_output/
"""


class OutputFormatter:
    """Module used to generate the results received into a single specific format (JSON, csv, xml, etc)."""

    def format(self, premise_output: "PremiseOutput"):
        """Abstract method for transforming the given PremiseOutput into the desired format."""
        pass

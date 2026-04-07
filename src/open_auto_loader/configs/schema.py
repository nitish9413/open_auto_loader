from enum import Enum


class SchemaEvolutionMode(Enum):
    """
    Controls how the loader behaves when source files
    don't match the locked schema_contract.json.
    """

    ADD_NEW_COLUMNS = "addNewColumns"  # Update JSON + Merge Delta
    FAIL_ON_NEW_COLUMNS = "failOnNewColumns"  # Raise SchemaMismatchError
    RESCUE = "rescue"  # Move extra data to _rescued_data
    NONE = "none"  # Ignore extra columns silently

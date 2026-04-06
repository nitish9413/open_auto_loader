"""
OpenAutoLoader: A high-performance, incremental data ingestion library
built on Polars and Delta Lake.
"""

# Define what is accessible when someone imports *
from .configs.storage import AWSConfig, AzureConfig, GCSConfig
from .main import OpenAutoLoader

__all__ = ["AWSConfig", "AzureConfig", "GCSConfig", "OpenAutoLoader"]

# Package Version
__version__ = "0.1.0"

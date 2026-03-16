# src/polars_autoloader/__init__.py
__version__ = "0.1.0"
from .main import OpenAutoLoader


def hello():
    return "Hello From Polars Autoloader!"


__all__ = ["OpenAutoLoader"]

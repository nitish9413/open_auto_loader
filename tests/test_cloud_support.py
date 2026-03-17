import pytest
from unittest.mock import MagicMock, patch
from open_auto_loader import OpenAutoLoader

def test_s3_storage_options_normalization():
    """Verify that S3 options are correctly split for Scanner and Engine."""
    source = "s3://test-bucket/data/"
    storage_options = {
        "aws_access_key_id": "mock_key",
        "aws_secret_access_key": "mock_secret",
        "aws_region": "ap-south-1"
    }

    # We patch the modules to avoid actual network/filesystem calls
    with patch("open_auto_loader.main.PolarsEngine"), \
         patch("open_auto_loader.main.FileScanner"), \
         patch("open_auto_loader.main.CheckPointManager"), \
         patch("open_auto_loader.main.SchemaManager"):

        loader = OpenAutoLoader(
            source=source,
            target="./target",
            check_point="./cp.db",
            schema_path="./schema",
            storage_options=storage_options
        )

        # Check Scanner Options (Should have 'key' and 'secret')
        assert loader.scanner_options["key"] == "mock_key"
        assert loader.scanner_options["client_kwargs"]["region_name"] == "ap-south-1"
        assert "aws_access_key_id" not in loader.scanner_options

        # Check Engine Options (Should have 'aws_access_key_id')
        assert loader.engine_options["aws_access_key_id"] == "mock_key"
        assert loader.engine_options["region"] == "ap-south-1"

def test_unsupported_protocol_raises_error():
    """Verify that a helpful error is raised if a driver is missing."""
    # We mock fsspec.filesystem to raise a ValueError (as it does when a protocol is unknown)
    with patch("fsspec.filesystem", side_effect=ValueError("Protocol not known")):
        from open_auto_loader.scanner import FileScanner

        with pytest.raises(ImportError, match="Protocol 'unknown' is not supported"):
            FileScanner(source_dir="unknown://path")

def test_ensure_protocol_helper():
    """Test the internal _ensure_protocol logic in FileScanner."""
    from open_auto_loader.scanner import FileScanner

    # Mock fsspec to allow initialization
    with patch("fsspec.filesystem"):
        scanner = FileScanner(source_dir="s3://my-bucket/files")

        # Should add protocol if missing
        assert scanner._ensure_protocol("my-bucket/files/data.csv") == "s3://my-bucket/files/data.csv"
        # Should not double-add protocol
        assert scanner._ensure_protocol("s3://my-bucket/files/data.csv") == "s3://my-bucket/files/data.csv"

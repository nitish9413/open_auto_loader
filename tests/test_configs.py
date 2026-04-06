import pytest

from open_auto_loader.configs.storage import AWSConfig


def test_aws_config_translation():
    """Verify AWSConfig translates pydantic fields to fsspec/polars keys."""
    config = AWSConfig(
        aws_access_key_id="ABC", aws_secret_access_key="XYZ", region_name="ap-south-1"
    )

    options = config.get_options()

    # Check fsspec keys
    assert options["key"] == "ABC"
    assert options["secret"] == "XYZ"

    # Check Polars keys
    assert options["aws_access_key_id"] == "ABC"
    assert options["aws_secret_access_key"] == "XYZ"
    assert options["region"] == "ap-south-1"


def test_invalid_config_raises_error():
    """Ensure Pydantic catches missing required fields."""
    with pytest.raises(ValueError):
        # Missing secret_access_key
        AWSConfig(aws_access_key_id="ABC")

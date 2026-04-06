from open_auto_loader import OpenAutoLoader
from open_auto_loader.configs.storage import AWSConfig, AzureConfig, GCSConfig


def run_aws_example() -> None:
    """Example: Ingesting from S3 to a Delta Lake on S3."""
    aws_storage = AWSConfig(
        aws_access_key_id="AKIA...",
        aws_secret_access_key="SECRET_KEY",
        region_name="us-east-1",
    )

    loader = OpenAutoLoader(
        source="s3://my-raw-bucket/json_data/",
        target="s3://my-silver-bucket/delta_table/",
        check_point="./metadata/s3_checkpoint.db",
        schema_path="./metadata/s3_schema.json",
        format_type="json",
        storage_config=aws_storage,
    )
    loader.run(batch_id="aws_batch_001")


def run_azure_example() -> None:
    """Example: Ingesting from Azure Blob Storage (ABFSS)."""
    azure_storage = AzureConfig(
        account_name="my_storage_account", account_key="AZURE_SECRET_KEY"
    )

    loader = OpenAutoLoader(
        source="abfss://container@account.dfs.core.windows.net/raw/",
        target="abfss://container@account.dfs.core.windows.net/gold/",
        check_point="./metadata/az_checkpoint.db",
        schema_path="./metadata/az_schema.json",
        storage_config=azure_storage,
    )
    loader.run(batch_id="azure_batch_001")


def run_gcs_example() -> None:
    """Example: Ingesting from Google Cloud Storage."""
    gcs_storage = GCSConfig(
        project_id="my-gcp-project",
        token="/path/to/service_account.json",  # Or "google_default"
    )

    loader = OpenAutoLoader(
        source="gs://my-gcs-bucket/incoming/",
        target="gs://my-gcs-bucket/table/",
        check_point="./metadata/gcs_checkpoint.db",
        schema_path="./metadata/gcs_schema.json",
        storage_config=gcs_storage,
    )
    loader.run(batch_id="gcs_batch_001")


if __name__ == "__main__":
    print("Select a cloud example to view logic. (Requires real credentials to run)")

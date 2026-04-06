from open_auto_loader.configs.storage import AWSConfig, AzureConfig, GCSConfig


def get_storage_config(
    source: str, options: dict | None
) -> AWSConfig | AzureConfig | GCSConfig | dict:
    """
    Factory to return the correct Pydantic model based on the URI scheme.
    """
    if not options:
        return {}

    if source.startswith("s3://"):
        return AWSConfig(**options)

    if source.startswith("abfss://") or source.startswith("az://"):
        return AzureConfig(**options)

    if source.startswith("gs://") or source.startswith("gcs://"):
        return GCSConfig(**options)

    # Fallback for local files or unsupported schemes
    return options

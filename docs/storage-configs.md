# ☁️ Cloud Storage Configurations

OpenAutoLoader uses type-safe configurations for AWS, Azure, and GCP.

## AWS S3
```python
from open_auto_loader.configs.storage import AWSConfig

config = AWSConfig(
    aws_access_key_id="...",
    aws_secret_access_key="...",
    region_name="us-east-1"
)
```

## Azure Blob Storage (ABFSS)

```python
from open_auto_loader.configs.storage import AzureConfig

config = AzureConfig(
    account_name="my_storage",
    account_key="secret_key"
)
```


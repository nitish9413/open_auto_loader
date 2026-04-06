from pydantic import BaseModel, SecretStr


class StorageConfig(BaseModel):
    """Base Configuration for all Cloud Providers."""

    endpoint_url: str | None = None

    def get_options(self) -> dict:
        """Base method to dump the model to a dictionary."""
        return self.model_dump(exclude_none=True, mode="json")


class AWSConfig(StorageConfig):
    aws_access_key_id: str
    aws_secret_access_key: SecretStr
    region_name: str = "us-east-1"
    aws_session_token: SecretStr | None = None

    def get_options(self) -> dict:
        """
        Translates AWS specific keys to a unified format
        accepted by both fsspec (Scanner) and Polars (Engine).
        """
        # Start with the standard fields
        options = super().get_options()

        # 1. Standardize for fsspec/Scanner (uses 'key' and 'secret')
        options["key"] = self.aws_access_key_id
        options["secret"] = self.aws_secret_access_key.get_secret_value()

        # 2. Standardize for Polars (uses 'aws_access_key_id')
        # (It's already there from the model fields,
        # but we ensure secret is stringified)
        options["aws_secret_access_key"] = self.aws_secret_access_key.get_secret_value()

        # 3. Handle Token if exists
        if self.aws_session_token:
            options["token"] = self.aws_session_token.get_secret_value()
            options["aws_session_token"] = self.aws_session_token.get_secret_value()

        # 4. Standardize Region
        options["region"] = self.region_name

        return options


class AzureConfig(StorageConfig):
    account_name: str
    account_key: SecretStr | None = None
    sas_token: SecretStr | None = None
    connection_string: SecretStr | None = None

    def get_options(self) -> dict:
        options = super().get_options()

        # Translate for fsspec/Polars commonality
        if self.account_key:
            options["account_key"] = self.account_key.get_secret_value()
        if self.sas_token:
            options["sas_token"] = self.sas_token.get_secret_value()

        return options


class GCSConfig(StorageConfig):
    project_id: str
    token: str | None = None  # Path to service account JSON or 'google_default'

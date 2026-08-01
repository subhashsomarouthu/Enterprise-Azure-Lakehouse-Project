# Security And Access

## Key Vault

Secrets are stored in Azure Key Vault.

Current secrets:

- azure-sql-admin-password

ADF uses its system-assigned managed identity to read secrets from Key Vault.

## ADF Managed Identity Permissions

ADF managed identity requires:

- Key Vault Secrets User on the Key Vault
- Storage Blob Data Contributor on the ADLS Gen2 storage account

## Storage Access

ADF writes source extracts to ADLS raw container.

Databricks will later read raw data and write Bronze/Silver/Gold Delta tables through Unity Catalog external locations or managed identities.

## Real Company Notes

Secrets should not be stored in Git.
Terraform should manage RBAC assignments where possible.
Production environments should use least privilege, private networking, and managed identities.

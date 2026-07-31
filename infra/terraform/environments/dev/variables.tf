variable "subscription_id" {
  description = "Azure subscription ID used for deployment."
  type        = string
}

variable "location" {
  description = "Azure region for all resources."
  type        = string
  default     = "Canada Central"
}

variable "environment" {
  description = "Deployment environment."
  type        = string
  default     = "dev"
}

variable "project_code" {
  description = "Short project code used in resource names."
  type        = string
  default     = "ealh"
}

variable "resource_group_name" {
  description = "Resource group name."
  type        = string
  default     = "rg-ealh-dev-canadacentral"
}

variable "storage_account_name" {
  description = "Globally unique ADLS Gen2 storage account name."
  type        = string
}

variable "data_factory_name" {
  description = "Azure Data Factory name."
  type        = string
  default     = "adf-ealh-dev-canadacentral"
}

variable "key_vault_name" {
  description = "Azure Key Vault name. Must be globally unique."
  type        = string
}

variable "databricks_workspace_name" {
  description = "Azure Databricks workspace name."
  type        = string
  default     = "dbw-ealh-dev-canadacentral"
}

variable "databricks_managed_resource_group_name" {
  description = "Managed resource group used by Azure Databricks."
  type        = string
  default     = "rg-ealh-dev-databricks-managed"
}

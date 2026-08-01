terraform {
  required_version = ">= 1.6.0"

  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 4.0"
    }
  }
}

provider "azurerm" {
  features {}

  subscription_id = var.subscription_id
}

data "azurerm_client_config" "current" {}

resource "azurerm_resource_group" "main" {
  name     = var.resource_group_name
  location = var.location

  tags = {
    environment = var.environment
    project     = var.project_code
    owner       = "data-engineering"
  }
}

resource "azurerm_storage_account" "lake" {
  name                            = var.storage_account_name
  resource_group_name             = azurerm_resource_group.main.name
  location                        = azurerm_resource_group.main.location
  account_tier                    = "Standard"
  account_replication_type        = "LRS"
  account_kind                    = "StorageV2"
  is_hns_enabled                  = true
  min_tls_version                 = "TLS1_2"
  allow_nested_items_to_be_public = false

  tags = {
    environment = var.environment
    project     = var.project_code
    layer       = "data-lake"
  }
}

resource "azurerm_storage_container" "containers" {
  for_each = toset([
    "raw",
    "bronze",
    "silver",
    "gold",
    "checkpoint",
    "landing",
    "archive",
    "quarantine"
  ])

  name                  = each.key
  storage_account_id    = azurerm_storage_account.lake.id
  container_access_type = "private"
}

resource "azurerm_data_factory" "main" {
  name                = var.data_factory_name
  location            = azurerm_resource_group.main.location
  resource_group_name = azurerm_resource_group.main.name

  identity {
    type = "SystemAssigned"
  }

  tags = {
    environment = var.environment
    project     = var.project_code
    component   = "orchestration"
  }
}

resource "azurerm_key_vault" "main" {
  name                       = var.key_vault_name
  location                   = azurerm_resource_group.main.location
  resource_group_name        = azurerm_resource_group.main.name
  tenant_id                  = data.azurerm_client_config.current.tenant_id
  sku_name                   = "standard"
  rbac_authorization_enabled = true

  tags = {
    environment = var.environment
    project     = var.project_code
    component   = "secrets"
  }
}

resource "azurerm_databricks_workspace" "main" {
  name                        = var.databricks_workspace_name
  resource_group_name         = azurerm_resource_group.main.name
  location                    = azurerm_resource_group.main.location
  sku                         = "premium"
  managed_resource_group_name = var.databricks_managed_resource_group_name

  tags = {
    environment = var.environment
    project     = var.project_code
    component   = "compute"
  }
}


resource "azurerm_mssql_server" "source" {
  name                         = var.sql_server_name
  resource_group_name          = azurerm_resource_group.main.name
  location                     = azurerm_resource_group.main.location
  version                      = "12.0"
  administrator_login          = var.sql_admin_login
  administrator_login_password = var.sql_admin_password
  minimum_tls_version          = "1.2"

  tags = {
    environment = var.environment
    project     = var.project_code
    component   = "source-system"
  }
}

resource "azurerm_mssql_database" "source" {
  name      = var.sql_database_name
  server_id = azurerm_mssql_server.source.id
  sku_name  = "Basic"

  tags = {
    environment = var.environment
    project     = var.project_code
    component   = "source-database"
  }
}

resource "azurerm_mssql_firewall_rule" "allow_azure_services" {
  name             = "AllowAzureServices"
  server_id        = azurerm_mssql_server.source.id
  start_ip_address = "0.0.0.0"
  end_ip_address   = "0.0.0.0"
}
output "resource_group_name" {
  value = azurerm_resource_group.main.name
}

output "storage_account_name" {
  value = azurerm_storage_account.lake.name
}

output "data_factory_name" {
  value = azurerm_data_factory.main.name
}

output "key_vault_name" {
  value = azurerm_key_vault.main.name
}

output "databricks_workspace_name" {
  value = azurerm_databricks_workspace.main.name
}

output "databricks_workspace_url" {
  value = azurerm_databricks_workspace.main.workspace_url
}

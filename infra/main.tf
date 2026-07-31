terraform {
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 4.21"
    }
  }
}

provider "azurerm" {
  features {}
  resource_provider_registrations = "none"
}

data "azurerm_resource_group" "main" {
  name = var.resource_group
}

resource "azurerm_storage_account" "main" {
  name                     = var.storage_account
  resource_group_name      = data.azurerm_resource_group.main.name
  location                 = data.azurerm_resource_group.main.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
  account_kind             = "StorageV2"
  is_hns_enabled           = true
  access_tier              = "Hot"
  allow_nested_items_to_be_public  = false
  cross_tenant_replication_enabled = false
}

resource "azurerm_service_plan" "main" {
  name                = var.service_plan
  resource_group_name = data.azurerm_resource_group.main.name
  location            = data.azurerm_resource_group.main.location
  os_type             = "Linux"
  sku_name            = "FC1"
}

resource "azurerm_storage_container" "deployment" {
  name                  = var.storage_container
  storage_account_id  = azurerm_storage_account.main.id
  container_access_type = "private"
}

resource "azurerm_function_app_flex_consumption" "main" {
  name                = var.function_app_name
  resource_group_name = data.azurerm_resource_group.main.name
  location            = data.azurerm_resource_group.main.location
  service_plan_id     = azurerm_service_plan.main.id
  client_certificate_mode = "Required"
  https_only = true
  webdeploy_publish_basic_authentication_enabled = false

  storage_container_type       = "blobContainer"
  storage_container_endpoint   = azurerm_storage_container.deployment.id
  storage_authentication_type  = "StorageAccountConnectionString"
  storage_access_key           = azurerm_storage_account.main.primary_access_key

  runtime_name    = "python"
  runtime_version = "3.11"

  maximum_instance_count = 100
  instance_memory_in_mb  = 512

  app_settings = {
    "AZURE_CONNECTION_STRING"              = azurerm_storage_account.main.primary_connection_string
    "AZURE_CONTAINER"                      = "bronze"
    "DEPLOYMENT_STORAGE_CONNECTION_STRING" = azurerm_storage_account.main.primary_connection_string
    "SPOTIFY_CLIENT_ID"                    = var.spotify_client_id
    "SPOTIFY_CLIENT_SECRET"                = var.spotify_client_secret
  }

site_config {
    application_insights_connection_string = var.app_insights_connection_string
    ip_restriction_default_action                 = "Allow"
    scm_ip_restriction_default_action             = "Allow"
    cors {
        allowed_origins     = ["https://portal.azure.com"]
        support_credentials = false
    }
}

lifecycle {
    ignore_changes = [tags]
  }
}
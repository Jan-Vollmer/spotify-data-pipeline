variable "spotify_client_id" {
  type      = string
  sensitive = true
}
variable "spotify_client_secret" {
  type      = string
  sensitive = true
}

variable "app_insights_connection_string" {
  type      = string
  sensitive = true
}

variable "resource_group" {
  type      = string
}

variable "storage_account" {
  type      = string
}

variable "service_plan" {
  type      = string
}

variable "storage_container" {
  type      = string
}

variable "function_app_name" {
  type      = string
}
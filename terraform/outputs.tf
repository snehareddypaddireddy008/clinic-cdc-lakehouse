output "lakehouse_bucket" {
  value = module.lakehouse_storage.bucket_name
}

output "iceberg_warehouse_uri" {
  description = "Paste into .env as ICEBERG_WAREHOUSE."
  value       = "s3://${module.lakehouse_storage.bucket_name}/warehouse"
}

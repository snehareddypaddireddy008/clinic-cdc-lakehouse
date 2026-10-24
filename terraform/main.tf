# Provisions the lakehouse object storage.
# Locally it targets SeaweedFS (S3-compatible) on localhost:8333.
# Set use_local_s3 = false to create the same buckets in real AWS S3.

terraform {
  required_version = ">= 1.6"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region     = var.region
  access_key = var.use_local_s3 ? var.local_access_key : null
  secret_key = var.use_local_s3 ? var.local_secret_key : null

  # Only needed for the local S3-compatible endpoint.
  s3_use_path_style           = var.use_local_s3
  skip_credentials_validation = var.use_local_s3
  skip_requesting_account_id  = var.use_local_s3
  skip_metadata_api_check     = var.use_local_s3

  dynamic "endpoints" {
    for_each = var.use_local_s3 ? [1] : []
    content {
      s3 = var.local_s3_endpoint
    }
  }

  default_tags {
    tags = {
      project    = "clinic-cdc-lakehouse"
      managed_by = "terraform"
      env        = var.environment
    }
  }
}

module "lakehouse_storage" {
  source        = "./modules/lakehouse_storage"
  bucket_name   = "${var.bucket_prefix}-${var.environment}"
  force_destroy = var.environment != "prod"
}

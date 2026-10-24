# One bucket holds the Iceberg warehouse (data + metadata files).
# Versioning protects against accidental overwrites of Iceberg metadata.

variable "bucket_name" {
  type = string
}

variable "force_destroy" {
  description = "Allow terraform destroy to delete a non-empty bucket (never in prod)."
  type        = bool
  default     = false
}

resource "aws_s3_bucket" "lakehouse" {
  bucket        = var.bucket_name
  force_destroy = var.force_destroy
}

resource "aws_s3_bucket_versioning" "lakehouse" {
  bucket = aws_s3_bucket.lakehouse.id
  versioning_configuration {
    status = "Enabled"
  }
}

output "bucket_name" {
  value = aws_s3_bucket.lakehouse.bucket
}

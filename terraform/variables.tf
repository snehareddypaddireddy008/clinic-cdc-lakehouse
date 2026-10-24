variable "use_local_s3" {
  description = "Target the local SeaweedFS endpoint instead of AWS."
  type        = bool
  default     = true
}

variable "local_s3_endpoint" {
  description = "S3-compatible endpoint used when use_local_s3 is true."
  type        = string
  default     = "http://localhost:8333"
}

variable "local_access_key" {
  description = "Access key for the local endpoint (matches seaweedfs/s3.json)."
  type        = string
  default     = "lakehouse"
}

variable "local_secret_key" {
  description = "Secret key for the local endpoint (matches seaweedfs/s3.json)."
  type        = string
  default     = "lakehouse-secret"
  sensitive   = true
}

variable "region" {
  type    = string
  default = "us-east-1"
}

variable "environment" {
  description = "dev, test or prod. Used in the bucket name and tags."
  type        = string
  default     = "dev"

  validation {
    condition     = contains(["dev", "test", "prod"], var.environment)
    error_message = "environment must be dev, test or prod."
  }
}

variable "bucket_prefix" {
  type    = string
  default = "clinic-lakehouse"
}

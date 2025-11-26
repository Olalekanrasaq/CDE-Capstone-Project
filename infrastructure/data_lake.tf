locals {
  service = {
    Service-Name = "Airflow"
  }
}

resource "aws_s3_bucket" "cde_bootcamp" {
  bucket = "cde-capstone-olalekan"

  tags = merge(local.generic_tag, local.service)
}

resource "aws_s3_bucket" "state_file" {
  bucket = "cde-capstone-olalekan-statefile"

  tags = local.generic_tag
}

resource "aws_s3_bucket_versioning" "captone_bucket_ver" {
  bucket = aws_s3_bucket.cde_bootcamp.id
  versioning_configuration {
    status = "Enabled"
  }
}
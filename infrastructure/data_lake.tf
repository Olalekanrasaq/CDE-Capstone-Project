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

data "aws_iam_policy_document" "s3_int_policy" {
  statement {
    sid    = "Auto-Copy-Policy-01"
    effect = "Allow"

    principals {
      type        = "Service"
      identifiers = ["redshift.amazonaws.com"]
    }

    actions = [
      "s3:Get*",
      "s3:Put*",
      "s3:List*"
    ]

    resources = [
      "arn:aws:s3:::cde-capstone-olalekan",
      "arn:aws:s3:::cde-capstone-olalekan/*"
    ]

    condition {
      test     = "ArnLike"
      variable = "aws:SourceArn"
      values   = ["arn:aws:redshift:us-east-1:068355631913:integration:*"]
    }

    condition {
      test     = "StringEquals"
      variable = "aws:SourceAccount"
      values   = ["068355631913"]
    }
  }
}

resource "aws_s3_bucket_policy" "s3_event_integration" {
  bucket = aws_s3_bucket.cde_bootcamp.id
  policy = data.aws_iam_policy_document.s3_int_policy.json
}
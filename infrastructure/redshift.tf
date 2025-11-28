locals {
  redshift_tags = {
    Service-Name = "Redshift"
  }
}

# create a role for redshift cluster
resource "aws_iam_role" "redshift_role" {
  name = "capstone-redshift-role"

  # Terraform's "jsonencode" function converts a
  # Terraform expression result to valid JSON syntax.
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Sid    = ""
        Principal = {
          Service = "redshift.amazonaws.com"
        }
      },
    ]
  })

  tags = merge(local.redshift_tags, local.generic_tag)
}

# IAM Policy document
data "aws_iam_policy_document" "redshift_role_policy" {
  statement {
    sid = "S3ReadAndWrite"

    actions = [
      "s3:*List*",
      "s3:*Get*",
      "s3:*Put*"
    ]

    resources = [
      "arn:aws:s3:::cde-capstone-olalekan",
      "arn:aws:s3:::cde-capstone-olalekan/*",
    ]
  }
}

# create IAM policy resource
resource "aws_iam_policy" "redshift_policy" {
    name = "cde-capstone-redshift-policy"
    policy = data.aws_iam_policy_document.redshift_role_policy.json
}

# attach the policy to the IAM role
resource "aws_iam_policy_attachment" "redshift_role_policy_attach" {
    name = "cde-capstone-redshift-policy-attach"
    roles = [aws_iam_role.redshift_role.name]
    policy_arn = aws_iam_policy.redshift_policy.arn
}

# get redshift username from SSM
data "aws_ssm_parameter" "redshift_db_username" {
  name = "/cdecapstone/redshift/db_username"
}

# get redshift password from SSM
data "aws_ssm_parameter" "redshift_db_password" {
  name = "/cdecapstone/redshift/db_password"
}

# provision a redshift cluster
resource "aws_redshift_cluster" "cde_capstone_cluster" {
  cluster_identifier = "cde-capstone-cluster"
  database_name      = "cde_capstone"
  master_username    = data.aws_ssm_parameter.redshift_db_username.value
  master_password    = data.aws_ssm_parameter.redshift_db_password.value
  node_type           = "ra3.large"
  cluster_type        = "multi-node"
  number_of_nodes     = 2
  publicly_accessible = true
  skip_final_snapshot = true
  iam_roles           = [aws_iam_role.redshift_role.arn]

  tags = merge(local.generic_tag, local.redshift_tags)
}

resource "aws_redshift_integration" "s3_event_integration" {
  integration_name = "capstone-s3-event-integration"
  source_arn       = aws_s3_bucket.cde_bootcamp.arn
  target_arn       = aws_redshift_cluster.cde_capstone_cluster.cluster_namespace_arn
}

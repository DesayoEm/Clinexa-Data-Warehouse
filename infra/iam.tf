
resource "aws_iam_user" "airflow_local" {
  name = "airflow-local"

  tags = {
    Project = "Clinexa"
    Purpose = "Local development access to S3"
  }
}

resource "aws_iam_access_key" "airflow_local" {
  user = aws_iam_user.airflow_local.name
}

output "airflow_access_key_id" {
  value     = aws_iam_access_key.airflow_local.id
  sensitive = true
}

output "airflow_secret_access_key" {
  value     = aws_iam_access_key.airflow_local.secret
  sensitive = true
}

resource "aws_iam_policy" "s3_access" {
  name        = "s3-access"
  description = "Allow read/write to buckets"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:PutObject",
          "s3:GetObject",
          "s3:ListBucket",
          "s3:ReplicateObject",
          "s3:GetObjectAttributes",
          "s3:GetObjectVersion",
          "s3:RestoreObject",
          "s3:DeleteObject",
          "s3:HeadObject"
        ]
        Resource = [
          aws_s3_bucket.clinexa-ct.arn,
          "${aws_s3_bucket.clinexa-ct.arn}/*"
        ]
      }
    ]
  })
}

resource "aws_iam_user_policy_attachment" "airflow_s3" {
  user       = aws_iam_user.airflow_local.name
  policy_arn = aws_iam_policy.s3_access.arn
}
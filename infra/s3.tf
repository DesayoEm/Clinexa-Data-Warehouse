resource "aws_s3_bucket" "clinexa-ct" {
  bucket        = var.clinexa-bucket
  force_destroy = true # to be disabled

  tags = {
    Name        = "CT gov bucket"
    Environment = "Test"
  }
}



resource "aws_s3_bucket_versioning" "ctgov_versioning" {
  bucket = aws_s3_bucket.clinexa-ct.id
  versioning_configuration {
    status = "Enabled"
  }

  # lifecycle {
  #   prevent_destroy = true
  # }
}



#LIFECYCLE POLICIES
resource "aws_s3_bucket_lifecycle_configuration" "clinexa_lifecycle" {
  bucket = aws_s3_bucket.clinexa-ct.id

  rule {
    id = "ctgov-raw-transition"

    filter {
      prefix = "CTGOV/raw/"
    }

    transition {
      days          = 30
      storage_class = "STANDARD_IA"
    }

    status = "Enabled"
  }


  rule {
    id = "ctgov-staging-expiration"

    filter {
      prefix = "CTGOV/staging/"
    }

    expiration {
      days = 7
    }

    status = "Enabled"
  }

  rule {
    id = "airflow-logs-transition"

    filter {
      prefix = "airflow-logs/"
    }

    transition {
      days          = 30
      storage_class = "STANDARD_IA"
    }
    status = "Enabled"
  }

}




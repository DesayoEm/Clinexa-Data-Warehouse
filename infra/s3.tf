resource "aws_s3_bucket" "clinexa-ctgov" {
  bucket = "clinexa-ct"
  force_destroy = true #disabled in prod

  tags = {
    Name        = "CT gov bucket"
    Environment = "Test"
  }
}

resource "aws_s3_bucket_versioning" "ctgov_versioning" {
  bucket = aws_s3_bucket.clinexa-ctgov.id
  versioning_configuration {
    status = "Enabled"
  }

  # lifecycle {
  #   prevent_destroy = true
  # }
}


resource "aws_s3_bucket_lifecycle_configuration" "ct_gov_archive_lifecycle" {
  bucket = aws_s3_bucket.clinexa-ctgov.id

  rule {
    id     = "TransitionToDeepArchive"
    status = "Enabled"

    transition {
      days          = 7
      storage_class = "DEEP_ARCHIVE"
    }
  }

  }

#LOGS BUCKET

resource "aws_s3_bucket" "airflow-logs" {
  bucket = "clinexa-airflow-logs"
  force_destroy = true

  tags = {
    Name        = "logs"
  }
}

resource "aws_s3_bucket_versioning" "logs_versioning" {
  bucket = aws_s3_bucket.airflow-logs.id
  versioning_configuration {
    status = "Enabled"
  }
}


resource "aws_s3_bucket_lifecycle_configuration" "logs_lifecycle" {
  bucket = aws_s3_bucket.airflow-logs.id

  rule {
    id     = "TransitionToDeepArchive"
    status = "Enabled"

    transition {
      days          = 7
      storage_class = "DEEP_ARCHIVE"
    }
  }

  }



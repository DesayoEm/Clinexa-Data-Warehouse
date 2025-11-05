resource "aws_s3_bucket" "clinexa-ctgov-staging" {
  bucket = "clinexa-ctgov-staging"

  tags = {
    Name        = "CT gov bucket"
    Environment = "Test"
  }
}
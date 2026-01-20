terraform {
  backend "s3" {
    bucket = var.clinexa-bucket
    key    = "state/terraform.tfstate"
    region = var.region
  }
}

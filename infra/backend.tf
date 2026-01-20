terraform {
  backend "s3" {
    bucket = "clinexa-ct"
    key    = "terraform/terraform.tfstate"
    region = "eu-west-2"
  }
}

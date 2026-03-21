terraform {
  backend "s3" {
    bucket = "terraform-mpb"
    key    = "clinexa.tfstate"
    region = var.region
  }
}

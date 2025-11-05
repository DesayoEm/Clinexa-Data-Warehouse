

provider "aws" {
  region = "eu-west-2"
}


resource "aws_vpc" "clinexa-vpc" {
  cidr_block = "10.0.0.0/16"
}

resource "aws_subnet" "main-sub"{
  vpc_id = aws_vpc.clinexa-vpc.id
  cidr_block = "10.0.2.0/24"

  tags = {
    name = "Main"
  }
}
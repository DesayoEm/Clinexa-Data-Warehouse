variable "clinexa-bucket" {
  type    = string
  default = "clinexa-ct"
}


variable "log-bucket" {
  type    = string
  default = "clinexa-airflow-logs"
}

variable "region" {
  type        = string
  description = "AWS Region"
  default     = "eu-west-2"
}
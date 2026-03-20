variable "aws_region" {
  description = "AWS region where resources will be created"
  type = string
}

variable "availability_zone" {
  description = "Availability Zone for the Ignis subnet"
  type        = string
  default     = "us-east-1a"
}
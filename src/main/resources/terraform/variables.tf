variable "aws_region" {
  description = "AWS region where resources will be created"
  type = string
}

variable "availability_zone" {
  description = "Availability Zone for the Ignis subnet"
  type        = string
}

variable "ignis_ami_name_prefix" {
  description = "Prefix del nombre de tu AMI personalizada (para buscar la más reciente)"
  type        = string
  default     = "ami-ignis"
}
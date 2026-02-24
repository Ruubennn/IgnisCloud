variable "instance_name" {
  description = "Name of the EC2 instance."
  type        = string
  default     = "ignis-test"
}

variable "instance_type" {
  description = "EC2 instance type."
  type        = string
  default     = "t2.micro"
}

variable "ami_id" {
  description = "AWS AMI used"
  type = string
  default = "ami-df570af1"
}

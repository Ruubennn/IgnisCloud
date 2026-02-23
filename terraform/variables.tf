variable "instance_name" {
  description = "Nombre de la instancia EC2."
  type        = string
  default     = "learn-terraform"
}

variable "instance_type" {
  description = "Tipo de la instancia EC2."
  type        = string
  default     = "t2.micro"
}

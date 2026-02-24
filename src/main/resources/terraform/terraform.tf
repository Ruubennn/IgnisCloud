terraform {
  required_providers {
    aws = {
      source = "hashicorp/aws"
      version = "~> 5.92"
    }
  
  local = {
      source  = "hashicorp/local"
      version = "~> 2.5"
    }
  }

  required_version = ">= 1.2"
}
provider "aws" {
  region                      = "us-west-2" 
  access_key                  = "test"      
  secret_key                  = "test"
  skip_credentials_validation = true
  skip_metadata_api_check     = true
  skip_requesting_account_id  = true

  endpoints {
    ec2             = "http://localhost:4566"
    #vpc             = "http://localhost:4566"
    networkmanager = "http://localhost:4566"
  }
}

resource "aws_vpc" "Mi_VPC" {
  cidr_block = "10.0.0.0/16"

  tags = {
    Name = "Mi_VPC"
  }
}

resource "aws_subnet" "Mi_Subnet" {
  vpc_id = aws_vpc.Mi_VPC.id
  cidr_block = "10.0.1.0/24"

  tags = {
    Name = "Mi_Subnet"
  }
}

resource "aws_security_group" "Mi_SG" {
  name = "Mi_SG"
  description = "Este será mi Grupo de Seguridad"
  vpc_id = aws_vpc.Mi_VPC.id

  tags = {
    Name = "Mi_SG"
  }
}

resource "aws_vpc_security_group_ingress_rule" "http" {
  security_group_id = aws_security_group.Mi_SG.id
  cidr_ipv4 = "0.0.0.0/0"
  from_port = 80
  ip_protocol = "tcp"
  to_port = 80
}

resource "aws_vpc_security_group_ingress_rule" "ssh" {
  security_group_id = aws_security_group.Mi_SG.id
  cidr_ipv4 = "0.0.0.0/0"
  from_port = 22
  ip_protocol = "tcp"
  to_port = 22
}

resource "aws_vpc_security_group_egress_rule" "salida" {
  security_group_id = aws_security_group.Mi_SG.id
  cidr_ipv4         = "0.0.0.0/0"
  ip_protocol       = "-1"
}

resource "aws_instance" "Mi_EC2" {
  ami = "ami-df570af1"
  instance_type = var.instance_type

  tags = {
    Name = var.instance_name
  }
}

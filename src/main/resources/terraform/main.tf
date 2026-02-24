provider "aws" {
  region                      = "us-west-2"
  access_key                  = "test"
  secret_key                  = "test"
  skip_credentials_validation = true
  skip_metadata_api_check     = true
  skip_requesting_account_id  = true

  endpoints {
    ec2             = "http://localhost:4566"
    networkmanager  = "http://localhost:4566"
    iam             = "http://localhost:4566"
    s3              = "http://localhost:4566"
    sts             = "http://localhost:4566"
  }
}

// VPC
resource "aws_vpc" "ignis_vpc" {
  cidr_block = "10.0.0.0/16"
  tags = {
    Name = "ignis_vpc"
  }
}

// Subnet
resource "aws_subnet" "ignis_subnet" {
  vpc_id = aws_vpc.ignis_vpc.id
  cidr_block = "10.0.1.0/24"

  tags = {
    Name = "ignis_subnet"
  }
}

// Internet Gateway
resource "aws_internet_gateway" "ignis_igw" {
    vpc_id = aws_vpc.ignis_vpc.id
    tags = {
        Name = "ignis-igw"
    }
}

// Route Table
resource "aws_route_table" "ignis_route_table" {
  vpc_id = aws_vpc.ignis_vpc.id
  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.ignis_igw.id
  }
  tags = {
    Name = "ignis-route-table"
  }
}

// Route Table Association
resource "aws_route_table_association" "ignis_route_assoc" {
  subnet_id = aws_subnet.ignis_subnet.id
  route_table_id = aws_route_table.ignis_route_table.id
}

// Security Group
resource "aws_security_group" "ignis_sg" {
  vpc_id = aws_vpc.ignis_vpc.id
  name = "ignis-sg"
  description = "Security group for Ignis scheduler instances"

  ingress {
    from_port = 22
    to_port = 22
    protocol = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  ingress {
    from_port = 80
    to_port   = 80
    protocol  = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  egress {
    from_port = 0
    to_port = 0
    protocol = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name = "ignis-sg"
  }
}

// IAM Role
resource "aws_iam_role" "ignis_scheduler_role" {
  name = "ignis-scheduler-role"
  assume_role_policy =jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = {
        Service = "ec2.amazonaws.com"
      }
    }]
  })
}

// IAM Policy
resource "aws_iam_role_policy" "ignis_scheduler_policy" {
  name = "ignis-scheduler-policy"
  role = aws_iam_role.ignis_scheduler_role.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = [
        "ec2:RunInstances",
        "ec2:TerminateInstances",
        "ec2:DescribeInstances",
        "ec2:CreateTags"
      ]
      Resource = "*"
    }]
  })
}

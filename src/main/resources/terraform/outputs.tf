output "vpc_id" {
  value = aws_vpc.ignis_vpc.id
}

output "subnet_id" {
  value = aws_subnet.ignis_subnet.id
}

output "sg_id" {
  value = aws_security_group.ignis_sg.id
}

output "iam_role_arn" {
  value = aws_iam_role.ignis_scheduler_role.arn
}

output "jobs_bucket_name" {
  value = aws_s3_bucket.ignis_jobs.bucket
}
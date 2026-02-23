output "subnet_id" {
    description = "Identificador de la Subred: "
    value = aws_subnet.Mi_Subnet.id
}

output "sg_id" {
    description = "Identificador del Grupo de Seguridad: "
    value = aws_security_group.Mi_SG.id
}
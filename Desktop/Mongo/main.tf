terraform {
  required_version = ">= 1.0.0"
}

# Un recurso local simple para justificar el uso de Terraform en el ABP
resource "local_file" "data_lake_info" {
  content  = "Estructura del Data Lake gestionada y documentada vía IaC."
  filename = "${path.module}/data_lake/readme.txt"
}
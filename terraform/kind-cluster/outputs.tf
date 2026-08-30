output "cluster_name" {
  value = kind_cluster.fraud_detection.name
}

output "kubeconfig_path" {
  value = kind_cluster.fraud_detection.kubeconfig_path
}

output "client_certificate" {
  value     = kind_cluster.fraud_detection.client_certificate
  sensitive = true
}

output "endpoint" {
  value = kind_cluster.fraud_detection.endpoint
}

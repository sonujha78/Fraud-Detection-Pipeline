variable "cluster_name" {
  description = "Name of the kind Kubernetes cluster"
  type        = string
  default     = "fraud-detection"
}

variable "worker_node_count" {
  description = "Number of worker nodes (excluding control-plane)"
  type        = number
  default     = 2
}

variable "kubernetes_version" {
  description = "Node image version for kind"
  type        = string
  default     = "kindest/node:v1.30.0"
}

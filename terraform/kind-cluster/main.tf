provider "kind" {}

resource "kind_cluster" "fraud_detection" {
  name           = var.cluster_name
  wait_for_ready = true

  kind_config {
    kind        = "Cluster"
    api_version = "kind.x-k8s.io/v1alpha4"

    node {
      role  = "control-plane"
      image = var.kubernetes_version

      kubeadm_config_patches = [
        "kind: InitConfiguration\nnodeRegistration:\n  kubeletExtraArgs:\n    node-labels: \"ingress-ready=true\"\n"
      ]

      extra_port_mappings {
        container_port = 30080
        host_port       = 30080
        protocol        = "TCP"
      }

      extra_port_mappings {
        container_port = 30443
        host_port       = 30443
        protocol        = "TCP"
      }
    }

    dynamic "node" {
      for_each = range(var.worker_node_count)
      content {
        role  = "worker"
        image = var.kubernetes_version
      }
    }
  }
}

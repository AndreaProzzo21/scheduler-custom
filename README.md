# Context-Aware Kubernetes Scheduling for Industrial Digital Twins

Welcome to the repository for the **Context-Aware Kubernetes Scheduler**, a custom orchestration engine designed specifically for Industrial Internet of Things (IIoT) and Edge computing environments.

This project was developed as part of a Master's Thesis to overcome the limitations of the default Kubernetes scheduler (`kube-scheduler`), which relies primarily on static resource bin-packing (CPU/RAM). Instead, this solution introduces a **telemetry-driven, declarative scheduling approach** that dynamically routes Digital Twin workloads based on real-time physical conditions such as network latency, hardware temperature, and disk I/O saturation.

## Key Features

* **Telemetry-Driven Routing:** Integrates directly with the Prometheus monitoring stack (Node Exporter, Blackbox Exporter) to make scheduling decisions based on real-time infrastructural data.
* **Declarative Strategies:** Developers can request specific hardware optimizations simply by adding annotations to their Pod manifests (e.g., `iot-scheduler/strategy: "minimize-latency"`).
* **Resource Awareness & Safety Filters (V1):** Implements strict hard-filters to prevent workloads from being scheduled on nodes exceeding critical CPU or Memory thresholds, preventing cluster cascading failures.
* **Multi-Variable Weighted Scoring (V1):** Uses Min-Max mathematical normalization to combine custom telemetry metrics with standard node resource availability, ensuring a balanced cluster load.
* **Fault Tolerance & Graceful Degradation:** Features a robust fallback mechanism. If the telemetry layer (Prometheus) fails or a specific hardware sensor is missing, the scheduler safely falls back to evaluating standard cluster metrics via the K8s Metrics Server, ensuring zero-downtime operations.

## Repository Structure

The repository is organized into the following main directories:

* **`docs/`**: Contains the comprehensive, step-by-step academic documentation of the project's evolution.
* `01-intro-docs.md`: Project introduction and the rationale for a custom IIoT scheduler.
* `02-prototyping.md`: Foundational setup, resolving initial RBAC and K8s API integration bugs.
* `03-poc.md`: The Telemetry-Driven Proof of Concept, including Data Flow (Mermaid diagrams) and code breakdown.
* `04-poc-limitations.md`: Critical analysis of the PoC's structural limits (Resource Blindness, Herd Effect).
* `05-v1-scheduler.md`: *(Upcoming)* The final architectural implementation featuring weighted scoring and resilient watch-loops.


* **`scheduler/`**: Contains the Python source code of the custom scheduler engine (`scheduler_v1.py`) and the `Dockerfile` used for containerization.
* **`deployment/`**: Contains the Kubernetes YAML manifests required to deploy the scheduler.
* **`service-account/`**: Contains the rbac.yaml file defining the ClusterRoles, RoleBindings, and permissions required by the scheduler to interact with the API server.
* **`test/`**:Contains the dummy Pod manifests used to test the different scheduling strategies (e.g., latency, disk-io, temperature).

## Prerequisites

To run this custom scheduler in your own environment, the following components must be active in your Kubernetes cluster:

1. **Kubernetes Cluster** (v1.20+, tested on Talos Linux).
2. **K8s Metrics Server** (Required for the fallback engine and resource awareness).
3. **Prometheus Operator / Kube-Prometheus-Stack** (Required for telemetry data).
4. **Prometheus Blackbox Exporter** (Required for the `minimize-latency` ICMP/HTTP probing strategy).

## 🚀 Quick Start

1. **Deploy the RBAC and Scheduler:**
Apply the deployment manifests to create the necessary ServiceAccount, ClusterRole, and the Scheduler Deployment itself in the `kube-system` namespace.
```bash
kubectl apply -f service-account/rbac.yaml
kubectl apply -f deployment/scheduler-deploy.yaml

```


2. **Check the Engine Logs:**
Verify that the scheduler loop has started successfully.
```bash
kubectl logs -f deployment/iot-twin-scheduler -n kube-system

```


3. **Deploy a Digital Twin Pod:**
Create a Pod with the appropriate annotation. The scheduler will intercept it and calculate the best node.
```bash
kubectl apply -f test/test-latency.yaml

```



---




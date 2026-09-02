# 1. Introduction: The Need for Context-Aware Scheduling in IIoT

The advent of Industry 4.0 and the deployment of Digital Twins at the Edge require a paradigm shift in how computational workloads are orchestrated. While cloud environments offer virtually infinite and homogeneous resources, Edge computing environments are intrinsically heterogeneous, geographically distributed, and resource-constrained.

Kubernetes has established itself as the de facto standard for container orchestration, providing robust mechanisms for scaling and managing distributed architectures. However, the default Kubernetes scheduling component (`kube-scheduler`) is fundamentally designed for general-purpose IT workloads. Its decision-making engine is primarily driven by static resource bin-packing (evaluating CPU and Memory requests) and topological constraints (node selectors, affinity, and taints). While this approach is highly efficient for traditional microservices and web applications, it reveals significant limitations when applied to the Industrial Internet of Things (IIoT).

In the context of Digital Twins interacting with physical machinery, workloads possess strict, non-functional requirements that extend beyond standard compute resources. The default scheduler lacks "physical awareness" and cannot natively evaluate the real-time conditions of the physical environment. For example:

* **Network Latency:** A Digital Twin controlling a robotic arm requires ultra-low latency to the physical device. The standard scheduler cannot differentiate between a node with a 1ms ping and one with a 50ms ping.
* **Hardware Bottlenecks:** Edge databases or time-series data collectors (e.g., InfluxDB) require optimal Disk I/O. Deploying them on a node with a saturated storage drive leads to cascading failures, a metric the standard scheduler ignores.
* **Environmental Factors:** Edge hardware (such as industrial PCs or SBCs like Raspberry Pis) is susceptible to thermal throttling in factory environments. Standard orchestration does not factor in hardware temperature when allocating intensive computational tasks.

To bridge this gap, this project introduces a **Custom Context-Aware Kubernetes Scheduler** tailored specifically for IIoT Digital Twins. By shifting from a purely static allocation model to a dynamic, telemetry-driven approach, the custom scheduler integrates directly with the Prometheus monitoring stack. This allows developers to use a declarative approach (via Pod annotations) to define specific physical requirements, enabling the orchestrator to route workloads based on real-time network, thermal, and I/O conditions.

This documentation outlines the architectural design and the engineering evolution of the system, transitioning from an initial **Proof of Concept (PoC)**—which validated the feasibility of telemetry-based routing—to a robust, fault-tolerant **Version 1 (V1)**, featuring advanced multi-variable scoring, safety hard-filters, and graceful degradation mechanisms.

---

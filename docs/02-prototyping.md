# 2. Early Prototyping: Establishing the Orchestration Baseline

Before introducing advanced context-aware logic and telemetry integration, the project required a foundational phase to understand and reliably interact with the Kubernetes Control Plane. The objective of this initial stage was to build a rudimentary custom scheduler capable of intercepting and assigning Pods, completely stripped of any complex decision-making algorithms or external dependencies.

### 2.1 The Minimalist Scheduler

The first iteration of the scheduler did not utilize Pod annotations or Prometheus data. Its sole purpose was to prove the feasibility of replacing the `kube-scheduler` for specific workloads. The core workflow consisted of three basic steps:

1. **Event Interception:** Establishing a continuous connection to the Kubernetes API server using the `watch` stream. The script filtered for Pods in a `Pending` state that specifically requested `iot-twin-scheduler` in their `spec.schedulerName` field.
2. **Node Discovery:** Querying the API for a list of available nodes, filtering out those that were `NotReady`.
3. **Basic Allocation:** Assigning the intercepted Pod to the first available node in the list (a naive first-fit approach) without evaluating any performance metrics.

### 2.2 Overcoming Early Architectural Hurdles

While conceptually simple, this prototyping phase was crucial for uncovering and resolving several low-level infrastructural challenges and "sneaky" bugs inherent to custom Kubernetes controller development.

#### RBAC Permissions and Security

By default, Pods running inside a Kubernetes cluster have highly restricted access to the API server. Initial attempts to bind a Pod resulted in `403 Forbidden` errors. It was necessary to design a dedicated security context for the scheduler. This involved creating a specific `ServiceAccount`, a `ClusterRole` with explicit permissions to `get`, `list`, and `watch` nodes and pods, and critically, the permission to `create` the `pods/binding` subresource. Finally, a `ClusterRoleBinding` was implemented to tie these permissions to the scheduler's deployment.

#### The Python Client Deserialization Bug

The most insidious bug encountered during the early prototyping phase involved the actual Pod binding execution. When the script invoked the `create_namespaced_pod_binding` method, it frequently threw a fatal `ValueError`, causing the scheduler application to crash.

Upon deeper investigation of the API server logs and the Python Kubernetes client repository, it was discovered that this was a known deserialization bug within the client library itself. When a binding is successfully created, the Kubernetes API returns a `201 Created` status with a specific JSON string. However, the Python client attempts to parse this response against a strictly defined schema and fails, throwing a `ValueError` despite the operation being successful on the server side.

To resolve this without altering the underlying library, a targeted exception-handling mechanism was introduced:

```python
try:
    api_instance.create_namespaced_pod_binding(name=pod_name, namespace=namespace, body=binding)
except ValueError:
    # Safely handle the known Python K8s client deserialization bug.
    # The binding is actually successful on the API server.
    logging.info(f"Successfully bound POD [{pod_name}] to NODE [{node_name}] (Handled K8s Bug)")

```

Solving these foundational issues—proper RBAC authorization and robust API exception handling—paved the way for a stable execution loop. With the core orchestration baseline established, the project was ready to advance to the next phase: integrating Prometheus for telemetry-driven decision-making.

---


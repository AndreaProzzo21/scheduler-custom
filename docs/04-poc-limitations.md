# 4. Structural Limitations of the PoC

The Proof of Concept (PoC) successfully validated the core hypothesis: a custom Kubernetes scheduler could dynamically route IIoT workloads based on real-time environmental and network telemetry. However, while the PoC performed adequately in controlled, steady-state testing, a critical analysis of its architecture revealed several structural limitations. These flaws rendered the PoC unsuitable for production-grade Edge environments, necessitating a fundamental architectural evolution.

### 4.1 Single-Dimensional Evaluation (Resource Blindness)

The most severe limitation of the PoC was its "winner-takes-all", single-dimensional decision engine. When a Pod requested the `minimize-latency` strategy, the algorithm blindly searched for the absolute lowest network ping, completely ignoring the fundamental health metrics of the target node.

In a real-world scenario, a node might offer an excellent 1ms network latency but simultaneously suffer from 99% CPU utilization or severe RAM saturation. By optimizing exclusively for a single external metric, the PoC risked scheduling heavy Digital Twin workloads onto overloaded nodes, potentially triggering Out-Of-Memory (OOM) kills and cascading cluster failures. The lack of standard resource awareness (CPU and Memory) alongside the custom metrics was a critical design flaw.

### 4.2 Watch Loop Fragility and Exception Handling

Kubernetes control plane components must be highly resilient. The PoC relied on a simplistic, continuous `watch.Watch().stream()` loop. This approach exhibited significant fragility:

* **API Timeouts and "410 Gone":** The Kubernetes API server periodically terminates long-running watch connections (often returning a `410 Gone` HTTP status) or drops connections due to network blips. The PoC did not handle these disconnections, causing the scheduler process to exit silently.
* **Lack of Isolation:** If an unexpected exception occurred while parsing a malformed Pod annotation, the unhandled exception would crash the entire main loop, leaving all subsequent Pods in a perpetual `Pending` state.

### 4.3 Static Fallback and the "Herd Effect"

While the PoC introduced a fallback mechanism (querying the K8s Metrics Server if Prometheus failed), its ultimate safety net was deeply flawed. If both Prometheus and the Metrics Server were unreachable, the algorithm simply defaulted to the first available node in the list (`list(nodes_dict.keys())[0]`).

In the event of a total telemetry outage during a mass-deployment of Pods, this static fallback would route *every single Pod* to the exact same node. This phenomenon, known as the "Herd Effect," defeats the purpose of distributed orchestration and guarantees the rapid overload of the selected worker node. A true round-robin or randomized distribution mechanism was missing.

### 4.4 Blind Binding Verification

To bypass the known Python Kubernetes client deserialization bug, the PoC utilized a `try-except ValueError` block. However, it assumed that catching the error inherently meant the binding was successful on the API server. It lacked a verification mechanism to confirm that the Pod's `spec.nodeName` had actually been updated. In an unstable network, assuming success without eventual consistency checks is an anti-pattern in distributed systems design.

### 4.5 Transitioning to Version 1 (V1)

Addressing these limitations required moving beyond a simple scripting approach. The system needed to transition from an absolute minimum-value selector to a mature, multi-variable orchestrator.

To achieve production-readiness, the next iteration required:

1. **Safety Hard-Filters:** The ability to instantly exclude nodes that exceed safe CPU/RAM thresholds, regardless of their telemetry scores.
2. **Weighted Scoring Algorithms:** A mathematical normalization model to evaluate custom metrics (latency/temperature) *in conjunction* with standard resource availability.
3. **Resilient Execution:** Implementing exponential backoffs, API reconnects, and robust exception isolation.

These requirements laid the architectural blueprint for the final iteration of the project: **Version 1 (V1)**.

---
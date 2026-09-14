from flask import Flask, jsonify, request, send_file
from kubernetes import client, config
from kubernetes.client.rest import ApiException
from datetime import datetime, timezone
import requests
import os
import re
import logging

logging.basicConfig(level=logging.INFO)
app = Flask(__name__)

# --------------------------------------------------------------------------
# Configuration
# --------------------------------------------------------------------------
PROMETHEUS_URL = os.getenv("PROMETHEUS_URL", "http://prometheus-operated.monitoring.svc.cluster.local:9090")
SCHEDULER_NAME = os.getenv("SCHEDULER_NAME", "iot-twin-scheduler")

# The custom scheduler is itself deployed as a workload. These identify that
# Deployment so the dashboard can monitor the scheduler's own health, not
# just the pods it schedules.
SCHEDULER_DEPLOYMENT_NAME = os.getenv("SCHEDULER_DEPLOYMENT_NAME", "iot-twin-scheduler")
SCHEDULER_NAMESPACE = os.getenv("SCHEDULER_NAMESPACE", "kube-system")

# Best-effort metric names exposed by the scheduler's own /metrics endpoint.
# Adjust these to match your exporter - they degrade gracefully to "N/D" in
# the UI if the metric does not exist.
SCHED_METRIC_DECISIONS = os.getenv("SCHED_METRIC_DECISIONS", "sum(iot_scheduler_scheduling_decisions_total)")
SCHED_METRIC_ERRORS = os.getenv("SCHED_METRIC_ERRORS", "sum(iot_scheduler_scheduling_errors_total)")
SCHED_METRIC_LATENCY = os.getenv("SCHED_METRIC_LATENCY", "avg(iot_scheduler_scheduling_duration_seconds) * 1000")
SCHED_METRIC_QUEUE = os.getenv("SCHED_METRIC_QUEUE", "sum(iot_scheduler_pending_queue_size)")

try:
    config.load_incluster_config()
except Exception:
    config.load_kube_config()

v1_core = client.CoreV1Api()
v1_apps = client.AppsV1Api()

# --------------------------------------------------------------------------
# Validation rules for the reschedule form. Every strategy accepts a
# different *kind* of target value; this is enforced server-side so a bad
# request (e.g. an IP address for a temperature-aware strategy) can never
# reach the cluster, regardless of what the client sends.
# --------------------------------------------------------------------------
ALLOWED_STRATEGIES = {"default", "minimize-latency", "temperature-aware", "disk-io-aware"}

STRATEGY_TARGET_RULES = {
    "default": {"kind": "none"},
    "minimize-latency": {"kind": "ip"},
    "temperature-aware": {"kind": "number", "min": 0, "max": 120, "unit": "\u00b0C"},
    "disk-io-aware": {"kind": "number", "min": 0, "max": 10000, "unit": "MB/s"},
}

IP_RE = re.compile(r"^(\d{1,3})\.(\d{1,3})\.(\d{1,3})\.(\d{1,3})$")
HW_TAGS_RE = re.compile(r"^[a-zA-Z0-9,\-\s]*$")


def validate_reschedule_payload(data):
    """Returns a list of human readable error strings. Empty list = valid."""
    errors = []

    strategy = (data.get("strategy") or "default").strip()
    if strategy not in ALLOWED_STRATEGIES:
        errors.append(f"Unknown strategy '{strategy}'.")
        return errors

    rule = STRATEGY_TARGET_RULES[strategy]
    target = (data.get("target") or "").strip()

    if rule["kind"] == "none":
        if target:
            errors.append(f"Strategy '{strategy}' does not take a target value. Leave the target field empty.")

    elif rule["kind"] == "ip":
        if target:
            m = IP_RE.match(target)
            if not m or not all(0 <= int(octet) <= 255 for octet in m.groups()):
                errors.append("Target must be a valid IPv4 address (e.g. 192.168.1.10), or left empty to average across nodes.")

    elif rule["kind"] == "number":
        if target:
            try:
                value = float(target)
            except ValueError:
                errors.append(
                    f"Strategy '{strategy}' expects a numeric threshold in {rule['unit']}, not an IP address or free text."
                )
            else:
                if not (rule["min"] <= value <= rule["max"]):
                    errors.append(f"Target value must be between {rule['min']} and {rule['max']} {rule['unit']}.")

    hw_required = data.get("hw_required", "")
    if hw_required and not HW_TAGS_RE.match(hw_required):
        errors.append("Hardware tags may only contain letters, numbers, hyphens and commas.")

    return errors


# --------------------------------------------------------------------------
# Prometheus helpers
# --------------------------------------------------------------------------
def query_prometheus(query, label_key):
    """Runs a vector query, returns {label_value: metric_value}."""
    try:
        res = requests.get(f"{PROMETHEUS_URL}/api/v1/query", params={"query": query}, timeout=3)
        res.raise_for_status()
        data = res.json().get("data", {}).get("result", [])
        result = {}
        for item in data:
            label_val = item["metric"].get(label_key)
            if label_val:
                clean_key = label_val.split(":")[0] if label_key == "instance" else label_val
                result[clean_key] = float(item["value"][1])
        return result
    except Exception as e:
        logging.warning(f"Prometheus query failed for {label_key}: {e}")
        return {}


def query_prometheus_scalar(query):
    """Runs a scalar/aggregate query, returns a single float or None if unavailable."""
    try:
        res = requests.get(f"{PROMETHEUS_URL}/api/v1/query", params={"query": query}, timeout=3)
        res.raise_for_status()
        data = res.json().get("data", {}).get("result", [])
        if data:
            return float(data[0]["value"][1])
        return None
    except Exception as e:
        logging.warning(f"Prometheus scalar query failed ({query}): {e}")
        return None


# --------------------------------------------------------------------------
# Small formatting helpers
# --------------------------------------------------------------------------
def to_aware(dt):
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt


def human_age(dt):
    """Turns a datetime into a compact age string like '3d 4h' or '12m'."""
    dt = to_aware(dt)
    if dt is None:
        return "-"
    delta = datetime.now(timezone.utc) - dt
    seconds = int(delta.total_seconds())
    if seconds < 0:
        seconds = 0
    days, rem = divmod(seconds, 86400)
    hours, rem = divmod(rem, 3600)
    minutes, _ = divmod(rem, 60)
    if days > 0:
        return f"{days}d {hours}h"
    if hours > 0:
        return f"{hours}h {minutes}m"
    return f"{minutes}m"


# --------------------------------------------------------------------------
# Routes
# --------------------------------------------------------------------------
@app.route("/")
def index():
    return send_file("index.html")


@app.route("/api/dashboard", methods=["GET"])
def get_dashboard_data():
    try:
        # 1. Nodes
        nodes_info = {}
        for n in v1_core.list_node().items:
            name = n.metadata.name
            annotations = n.metadata.annotations or {}
            node_status = "Ready" if any(c.type == "Ready" and c.status == "True" for c in n.status.conditions) else "NotReady"
            internal_ip = next((addr.address for addr in n.status.addresses if addr.type == "InternalIP"), name)

            nodes_info[name] = {
                "name": name,
                "ip": internal_ip,
                "status": node_status,
                "hw_tags": annotations.get("iot-scheduler/hw-tags", ""),
                "cpu_pct": 0.0,
                "ram_pct": 0.0,
            }

        cpu_usage = query_prometheus(
            '100 - (avg by (instance) (rate(node_cpu_seconds_total{mode="idle"}[2m])) * 100)', "instance"
        )
        ram_usage = query_prometheus(
            "100 * (1 - ((node_memory_MemAvailable_bytes) / (node_memory_MemTotal_bytes)))", "instance"
        )

        for name, data in nodes_info.items():
            data["cpu_pct"] = round(cpu_usage.get(data["ip"], 0.0), 1)
            data["ram_pct"] = round(ram_usage.get(data["ip"], 0.0), 1)

        # 2. Pod-level metrics (reused for every managed workload)
        pod_cpu_cores = query_prometheus('sum(rate(container_cpu_usage_seconds_total{container!=""}[2m])) by (pod)', "pod")
        pod_ram_bytes = query_prometheus('sum(container_memory_working_set_bytes{container!=""}) by (pod)', "pod")

        # 3. Deployments managed by the custom scheduler
        workloads = []
        status_counts = {"Running": 0, "Pending": 0, "Other": 0}
        strategy_counts = {}

        for d in v1_apps.list_deployment_for_all_namespaces().items:
            if d.spec.template.spec.scheduler_name != SCHEDULER_NAME:
                continue

            annotations = d.spec.template.metadata.annotations or {}
            strategy = annotations.get("iot-scheduler/strategy", "default")

            pod_name = ""
            current_node = "Pending"
            status = "Unknown"
            cpu_mc = 0.0
            ram_mb = 0.0
            restart_count = 0

            try:
                match_labels = d.spec.selector.match_labels
                label_selector = ",".join([f"{k}={v}" for k, v in match_labels.items()])
                pods = v1_core.list_namespaced_pod(namespace=d.metadata.namespace, label_selector=label_selector).items

                if pods:
                    active_pod = pods[0]
                    pod_name = active_pod.metadata.name
                    current_node = active_pod.spec.node_name or "Pending"
                    status = active_pod.status.phase
                    container_statuses = active_pod.status.container_statuses or []
                    restart_count = sum(cs.restart_count for cs in container_statuses)

                    cpu_mc = round(pod_cpu_cores.get(pod_name, 0.0) * 1000, 1)
                    ram_mb = round(pod_ram_bytes.get(pod_name, 0.0) / (1024 * 1024), 1)

            except Exception as e:
                logging.error(f"Error fetching pod for {d.metadata.name}: {e}")

            if status == "Running":
                status_counts["Running"] += 1
            elif status == "Pending":
                status_counts["Pending"] += 1
            else:
                status_counts["Other"] += 1

            strategy_counts[strategy] = strategy_counts.get(strategy, 0) + 1

            workloads.append(
                {
                    "deployment_name": d.metadata.name,
                    "pod_name": pod_name,
                    "namespace": d.metadata.namespace,
                    "current_node": current_node,
                    "status": status,
                    "restart_count": restart_count,
                    "cpu_usage_mc": cpu_mc,
                    "ram_usage_mb": ram_mb,
                    "hw_required": annotations.get("iot-scheduler/hw-required", ""),
                    "strategy": strategy,
                    "target": annotations.get("iot-scheduler/strategy-target", ""),
                }
            )

        # 4. Cluster-wide summary (for the Overview tab)
        ready_nodes = sum(1 for n in nodes_info.values() if n["status"] == "Ready")
        avg_cpu = round(sum(n["cpu_pct"] for n in nodes_info.values()) / len(nodes_info), 1) if nodes_info else 0.0
        avg_ram = round(sum(n["ram_pct"] for n in nodes_info.values()) / len(nodes_info), 1) if nodes_info else 0.0

        # 5. Recent warning events cluster-wide (for the Overview alert feed)
        recent_events = []
        try:
            events = v1_core.list_event_for_all_namespaces(limit=200).items
            warnings = [e for e in events if e.type == "Warning"]
            warnings.sort(key=lambda e: to_aware(e.last_timestamp) or to_aware(e.event_time) or datetime.min.replace(tzinfo=timezone.utc), reverse=True)
            for e in warnings[:8]:
                recent_events.append(
                    {
                        "namespace": e.metadata.namespace,
                        "object": f"{e.involved_object.kind}/{e.involved_object.name}",
                        "reason": e.reason,
                        "message": e.message,
                        "count": e.count,
                        "age": human_age(e.last_timestamp or e.event_time),
                    }
                )
        except Exception as e:
            logging.warning(f"Could not fetch cluster events: {e}")

        return jsonify(
            {
                "nodes": list(nodes_info.values()),
                "workloads": workloads,
                "scheduler_stats": {
                    "total_managed": len(workloads),
                    "running": status_counts["Running"],
                    "pending": status_counts["Pending"],
                    "other": status_counts["Other"],
                    "strategies_used": list(strategy_counts.keys()),
                    "strategy_distribution": strategy_counts,
                },
                "cluster_summary": {
                    "total_nodes": len(nodes_info),
                    "ready_nodes": ready_nodes,
                    "avg_cpu_pct": avg_cpu,
                    "avg_ram_pct": avg_ram,
                },
                "recent_events": recent_events,
            }
        )
    except Exception as e:
        logging.error(f"Critical error in /api/dashboard: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/api/scheduler", methods=["GET"])
def get_scheduler_status():
    """Dedicated health/telemetry view of the iot-twin-scheduler Deployment itself."""
    result = {
        "deployment_name": SCHEDULER_DEPLOYMENT_NAME,
        "namespace": SCHEDULER_NAMESPACE,
        "found": False,
    }
    try:
        try:
            dep = v1_apps.read_namespaced_deployment(name=SCHEDULER_DEPLOYMENT_NAME, namespace=SCHEDULER_NAMESPACE)
        except ApiException as e:
            if e.status == 404:
                result["error"] = (
                    f"Deployment '{SCHEDULER_DEPLOYMENT_NAME}' not found in namespace '{SCHEDULER_NAMESPACE}'. "
                    "Set SCHEDULER_DEPLOYMENT_NAME / SCHEDULER_NAMESPACE env vars to match your install."
                )
                return jsonify(result)
            raise

        result["found"] = True
        spec_replicas = dep.spec.replicas or 0
        status = dep.status
        ready_replicas = status.ready_replicas or 0
        available_replicas = status.available_replicas or 0
        updated_replicas = status.updated_replicas or 0

        conditions = [
            {
                "type": c.type,
                "status": c.status,
                "reason": c.reason,
                "message": c.message,
            }
            for c in (status.conditions or [])
        ]

        available_ok = any(c["type"] == "Available" and c["status"] == "True" for c in conditions)
        if spec_replicas == 0:
            health = "Scaled to zero"
        elif ready_replicas >= spec_replicas and available_ok:
            health = "Healthy"
        elif ready_replicas > 0:
            health = "Degraded"
        else:
            health = "Down"

        container = dep.spec.template.spec.containers[0] if dep.spec.template.spec.containers else None
        resources = {}
        if container and container.resources:
            resources = {
                "requests": container.resources.requests or {},
                "limits": container.resources.limits or {},
            }

        # Pods backing the scheduler deployment
        match_labels = dep.spec.selector.match_labels
        label_selector = ",".join([f"{k}={v}" for k, v in match_labels.items()])
        pods = v1_core.list_namespaced_pod(namespace=SCHEDULER_NAMESPACE, label_selector=label_selector).items

        pod_names = [p.metadata.name for p in pods]
        pod_cpu_cores = query_prometheus('sum(rate(container_cpu_usage_seconds_total{container!=""}[2m])) by (pod)', "pod")
        pod_ram_bytes = query_prometheus('sum(container_memory_working_set_bytes{container!=""}) by (pod)', "pod")

        pod_list = []
        total_restarts = 0
        for p in pods:
            container_statuses = p.status.container_statuses or []
            restart_count = sum(cs.restart_count for cs in container_statuses)
            total_restarts += restart_count
            image = container_statuses[0].image if container_statuses else (container.image if container else "")
            ready = all(cs.ready for cs in container_statuses) if container_statuses else False

            pod_list.append(
                {
                    "pod_name": p.metadata.name,
                    "node": p.spec.node_name or "Pending",
                    "phase": p.status.phase,
                    "ready": ready,
                    "restart_count": restart_count,
                    "image": image,
                    "age": human_age(p.status.start_time),
                    "cpu_usage_mc": round(pod_cpu_cores.get(p.metadata.name, 0.0) * 1000, 1),
                    "ram_usage_mb": round(pod_ram_bytes.get(p.metadata.name, 0.0) / (1024 * 1024), 1),
                }
            )

        # Best-effort custom metrics exported by the scheduler binary itself
        custom_metrics = {
            "scheduling_decisions_total": query_prometheus_scalar(SCHED_METRIC_DECISIONS),
            "scheduling_errors_total": query_prometheus_scalar(SCHED_METRIC_ERRORS),
            "avg_scheduling_latency_ms": query_prometheus_scalar(SCHED_METRIC_LATENCY),
            "pending_queue_size": query_prometheus_scalar(SCHED_METRIC_QUEUE),
        }

        # Recent events involving the scheduler's own pods
        recent_events = []
        try:
            events = v1_core.list_namespaced_event(namespace=SCHEDULER_NAMESPACE).items
            relevant = [e for e in events if e.involved_object.name in pod_names or e.involved_object.name == SCHEDULER_DEPLOYMENT_NAME]
            relevant.sort(key=lambda e: to_aware(e.last_timestamp) or to_aware(e.event_time) or datetime.min.replace(tzinfo=timezone.utc), reverse=True)
            for e in relevant[:10]:
                recent_events.append(
                    {
                        "object": f"{e.involved_object.kind}/{e.involved_object.name}",
                        "type": e.type,
                        "reason": e.reason,
                        "message": e.message,
                        "count": e.count,
                        "age": human_age(e.last_timestamp or e.event_time),
                    }
                )
        except Exception as e:
            logging.warning(f"Could not fetch scheduler events: {e}")

        result.update(
            {
                "health": health,
                "image": container.image if container else "",
                "spec_replicas": spec_replicas,
                "ready_replicas": ready_replicas,
                "available_replicas": available_replicas,
                "updated_replicas": updated_replicas,
                "total_restarts": total_restarts,
                "age": human_age(dep.metadata.creation_timestamp),
                "conditions": conditions,
                "resources": resources,
                "pods": pod_list,
                "custom_metrics": custom_metrics,
                "recent_events": recent_events,
            }
        )
        return jsonify(result)

    except Exception as e:
        logging.error(f"Critical error in /api/scheduler: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/api/reschedule/<namespace>/<deployment_name>", methods=["POST"])
def update_and_reschedule(namespace, deployment_name):
    data = request.json or {}

    errors = validate_reschedule_payload(data)
    if errors:
        return jsonify({"status": "error", "errors": errors}), 400

    patch_body = {
        "spec": {
            "template": {
                "metadata": {
                    "annotations": {
                        "iot-scheduler/hw-required": data.get("hw_required", ""),
                        "iot-scheduler/strategy": data.get("strategy", "default"),
                        "iot-scheduler/strategy-target": data.get("target", ""),
                        "iot-scheduler/restartedAt": datetime.utcnow().isoformat(),
                    }
                }
            }
        }
    }
    try:
        v1_apps.patch_namespaced_deployment(name=deployment_name, namespace=namespace, body=patch_body)
        return jsonify({"status": "success"})
    except Exception as e:
        return jsonify({"status": "error", "errors": [str(e)]}), 500


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)

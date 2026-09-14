from flask import Flask, jsonify, request, send_file
from kubernetes import client, config
from datetime import datetime
import requests
import os
import logging

logging.basicConfig(level=logging.INFO)
app = Flask(__name__)

# Configurazione
PROMETHEUS_URL = os.getenv("PROMETHEUS_URL", "http://prometheus-operated.monitoring.svc.cluster.local:9090")
SCHEDULER_NAME = "iot-twin-scheduler"

try:
    config.load_incluster_config()
except:
    config.load_kube_config()

v1_core = client.CoreV1Api()
v1_apps = client.AppsV1Api()

def query_prometheus(query, label_key):
    """Esegue la query e restituisce un dizionario {label_value: metric_value}"""
    try:
        res = requests.get(f"{PROMETHEUS_URL}/api/v1/query", params={'query': query}, timeout=3)
        res.raise_for_status()
        data = res.json().get('data', {}).get('result', [])
        result = {}
        for item in data:
            label_val = item['metric'].get(label_key)
            if label_val:
                # Se è un IP (instance), rimuovi la porta
                clean_key = label_val.split(':')[0] if label_key == 'instance' else label_val
                result[clean_key] = float(item['value'][1])
        return result
    except Exception as e:
        logging.warning(f"Prometheus query failed for {label_key}: {e}")
        return {}

@app.route('/')
def index():
    return send_file('index.html')

@app.route('/api/dashboard', methods=['GET'])
def get_dashboard_data():
    try:
        # 1. Fetch Nodi e IP
        nodes_info = {}
        for n in v1_core.list_node().items:
            name = n.metadata.name
            annotations = n.metadata.annotations or {}
            node_status = 'Ready' if any(c.type == 'Ready' and c.status == 'True' for c in n.status.conditions) else 'NotReady'
            internal_ip = next((addr.address for addr in n.status.addresses if addr.type == 'InternalIP'), name)
            
            nodes_info[name] = {
                'name': name,
                'ip': internal_ip,
                'status': node_status,
                'hw_tags': annotations.get('iot-scheduler/hw-tags', ''),
                'cpu_pct': 0.0,
                'ram_pct': 0.0
            }

        # 2. Fetch Metriche Prometheus Livello Nodo
        cpu_usage = query_prometheus('100 - (avg by (instance) (rate(node_cpu_seconds_total{mode="idle"}[2m])) * 100)', 'instance')
        ram_usage = query_prometheus('100 * (1 - ((node_memory_MemAvailable_bytes) / (node_memory_MemTotal_bytes)))', 'instance')
        
        for name, data in nodes_info.items():
            data['cpu_pct'] = round(cpu_usage.get(data['ip'], 0.0), 1)
            data['ram_pct'] = round(ram_usage.get(data['ip'], 0.0), 1)

        # 3. Fetch Metriche Prometheus Livello Pod (Workloads)
        pod_cpu_cores = query_prometheus('sum(rate(container_cpu_usage_seconds_total{container!=""}[2m])) by (pod)', 'pod')
        pod_ram_bytes = query_prometheus('sum(container_memory_working_set_bytes{container!=""}) by (pod)', 'pod')

        # 4. Fetch Deployments & Assemblaggio
        workloads = []
        status_counts = {'Running': 0, 'Pending': 0, 'Other': 0}
        
        # FIX: rimosso le () dopo items
        for d in v1_apps.list_deployment_for_all_namespaces().items:
            if d.spec.template.spec.scheduler_name == SCHEDULER_NAME:
                annotations = d.spec.template.metadata.annotations or {}
                
                pod_name = ""
                current_node = "Pending"
                status = "Unknown"
                cpu_mc = 0.0
                ram_mb = 0.0
                
                try:
                    match_labels = d.spec.selector.match_labels
                    label_selector = ",".join([f"{k}={v}" for k, v in match_labels.items()])
                    pods = v1_core.list_namespaced_pod(namespace=d.metadata.namespace, label_selector=label_selector).items
                    
                    if pods:
                        active_pod = pods[0]
                        pod_name = active_pod.metadata.name
                        current_node = active_pod.spec.node_name or "Pending"
                        status = active_pod.status.phase
                        
                        # Recupero consumo reale da Prometheus
                        cpu_mc = round(pod_cpu_cores.get(pod_name, 0.0) * 1000, 1) # in millicores
                        ram_mb = round(pod_ram_bytes.get(pod_name, 0.0) / (1024 * 1024), 1) # in MB
                        
                except Exception as e:
                    logging.error(f"Errore recupero pod per {d.metadata.name}: {e}")

                # Aggiorna Statistiche Scheduler
                if status == 'Running':
                    status_counts['Running'] += 1
                elif status == 'Pending':
                    status_counts['Pending'] += 1
                else:
                    status_counts['Other'] += 1

                workloads.append({
                    'deployment_name': d.metadata.name,
                    'pod_name': pod_name,
                    'namespace': d.metadata.namespace,
                    'current_node': current_node,
                    'status': status,
                    'cpu_usage_mc': cpu_mc,
                    'ram_usage_mb': ram_mb,
                    'hw_required': annotations.get('iot-scheduler/hw-required', ''),
                    'strategy': annotations.get('iot-scheduler/strategy', 'default'),
                    'target': annotations.get('iot-scheduler/strategy-target', '')
                })

        return jsonify({
            'nodes': list(nodes_info.values()),
            'workloads': workloads,
            'scheduler_stats': {
                'total_managed': len(workloads),
                'running': status_counts['Running'],
                'pending': status_counts['Pending'],
                'strategies_used': list(set([w['strategy'] for w in workloads]))
            }
        })
    except Exception as e:
        logging.error(f"Errore critico in /api/dashboard: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/reschedule/<namespace>/<deployment_name>', methods=['POST'])
def update_and_reschedule(namespace, deployment_name):
    data = request.json
    patch_body = {
        "spec": {
            "template": {
                "metadata": {
                    "annotations": {
                        "iot-scheduler/hw-required": data.get('hw_required', ''),
                        "iot-scheduler/strategy": data.get('strategy', 'default'),
                        "iot-scheduler/strategy-target": data.get('target', ''),
                        "iot-scheduler/restartedAt": datetime.utcnow().isoformat()
                    }
                }
            }
        }
    }
    try:
        v1_apps.patch_namespaced_deployment(name=deployment_name, namespace=namespace, body=patch_body)
        return jsonify({"status": "success"})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080)

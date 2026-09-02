from kubernetes import client, config, watch
import logging
import requests
import os
import math

# --- CONFIGURAZIONE ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
SCHEDULER_NAME = "iot-twin-scheduler"
PROMETHEUS_URL = os.getenv("PROMETHEUS_URL", "http://prometheus-operated.monitoring.svc.cluster.local:9090")

# --- KUBERNETES ACTIONS ---
def bind_pod(api_instance, pod_name, namespace, node_name):
    target = client.V1ObjectReference(api_version='v1', kind='Node', name=node_name)
    meta = client.V1ObjectMeta(name=pod_name)
    binding = client.V1Binding(target=target, metadata=meta)
    
    try:
        api_instance.create_namespaced_pod_binding(name=pod_name, namespace=namespace, body=binding)
        logging.info(f"✅ Successfully bound POD [{pod_name}] to NODE [{node_name}]")
    except ValueError:
        # Bug nativo della libreria Python di K8s
        logging.info(f"✅ Successfully bound POD [{pod_name}] to NODE [{node_name}]")
    except client.ApiException as e:
        logging.error(f"❌ Failed to bind pod {pod_name}: {e}")

def get_available_nodes(api_instance):
    """
    BLINDATURA: Ora restituisce un dizionario { 'nome_nodo': 'indirizzo_ip' }
    """
    nodes = api_instance.list_node()
    valid_nodes = {}
    
    for n in nodes.items:
        # 1. Verifica che sia Ready
        is_ready = any(c.type == 'Ready' and c.status == 'True' for c in n.status.conditions)
        
        # 2. Verifica che NON abbia Taint 'NoSchedule' (Protegge il Control Plane)
        is_schedulable = True
        if n.spec.taints:
            for taint in n.spec.taints:
                if taint.effect == 'NoSchedule':
                    is_schedulable = False
                    break
        
        if is_ready and is_schedulable:
            # Estrae l'InternalIP del nodo per incrociarlo con Prometheus
            internal_ip = next((addr.address for addr in n.status.addresses if addr.type == 'InternalIP'), None)
            if internal_ip:
                valid_nodes[n.metadata.name] = internal_ip
            
    return valid_nodes

# --- PROMETHEUS LOGIC ---
def get_prometheus_metric(query):
    try:
        response = requests.get(f"{PROMETHEUS_URL}/api/v1/query", params={'query': query}, timeout=3)
        response.raise_for_status()
        results = response.json().get('data', {}).get('result', [])
        
        node_metrics = {}
        for res in results:
            labels = res.get('metric', {})
            # Catturiamo sia il nome (se c'è) sia l'IP
            raw_target = labels.get('kubernetes_node') or labels.get('instance', '')
            
            if raw_target:
                # Pulizia: se c'è la porta (es. 192.168.0.61:9100), la togliamo
                clean_target = raw_target.split(':')[0]
                node_metrics[clean_target] = float(res['value'][1])
                
        return node_metrics
    except Exception as e:
        logging.warning(f"⚠️ Prometheus fetch failed or timeout: {e}")
        return None

# --- FALLBACK LOGIC ---
def fallback_metrics_server(nodes_dict):
    """Fallback 1: Uso RAM tramite Metrics Server (esclude il control plane)"""
    logging.info("🔄 Initiating Fallback 1: Querying K8s Metrics Server...")
    custom_api = client.CustomObjectsApi()
    try:
        metrics = custom_api.list_cluster_custom_object("metrics.k8s.io", "v1beta1", "nodes")
        
        best_node = list(nodes_dict.keys())[0]
        min_mem = math.inf
        
        for item in metrics.get('items', []):
            node_name = item['metadata']['name']
            if node_name in nodes_dict: # Verifica che sia nei nodi validi (no control plane)
                mem_str = item['usage']['memory'].replace('Ki', '')
                mem_usage = int(mem_str)
                if mem_usage < min_mem:
                    min_mem = mem_usage
                    best_node = node_name
                    
        logging.info(f"🔄 Fallback 1 SUCCESS: Best node by lowest RAM usage is {best_node}")
        return best_node
    except Exception as e:
        logging.warning(f"⚠️ Metrics Server not available: {e}")
        logging.info(f"🆘 Initiating Fallback 2: Basic Round-Robin")
        return list(nodes_dict.keys())[0]

# --- SCHEDULING ENGINE ---
def calculate_best_node(nodes_dict, annotations):
    strategy = annotations.get('iot-scheduler/strategy', 'default')
    logging.info(f"🎯 Pod requested strategy: [{strategy}]")
    
    metrics = None
    best_node = None
    best_value = math.inf 

    # 1. SCELTA DELLA QUERY
    if strategy == 'minimize-latency':
        query = 'avg(probe_duration_seconds) by (kubernetes_node, instance)'
        metrics = get_prometheus_metric(query)
        
    elif strategy == 'temperature-aware':
        query = 'avg(node_hwmon_temp_celsius) by (instance)'
        metrics = get_prometheus_metric(query)
        
    elif strategy == 'disk-io-aware':
        # Ottimizza per I/O (filtra i device fittizi e calcola sui dischi fisici)
        query = 'sum(rate(node_disk_io_time_seconds_total{device=~"sd.*|vd.*|nvme.*"}[2m])) by (instance)'
        metrics = get_prometheus_metric(query)

    # 2. VALUTAZIONE INCROCIATA (NOME o IP)
    if metrics is not None and len(metrics) > 0:
        for node_name, node_ip in nodes_dict.items():
            # BLINDATURA: Tenta prima col nome (Blackbox), poi con l'IP (Node Exporter)
            val = metrics.get(node_name)
            if val is None:
                val = metrics.get(node_ip, math.inf)
                
            logging.info(f"🧮 Node [{node_name}] (IP: {node_ip}) - Metric Value: {val}")
            
            if val < best_value:
                best_value = val
                best_node = node_name
                
        # Se tutti i nodi hanno restituito "inf", innesca il fallback
        if best_value == math.inf:
            logging.warning(f"⚠️ Data found, but no matching node IPs. Falling back...")
            return fallback_metrics_server(nodes_dict)
            
        logging.info(f"🏆 Strategy [{strategy}] selected WINNER node: {best_node}")
        return best_node
    else:
        logging.warning(f"⚠️ Strategy [{strategy}] failed (Prometheus unavailable or empty).")
        return fallback_metrics_server(nodes_dict)

# --- MAIN LOOP ---
def main():
    try:
        config.load_incluster_config()
    except config.ConfigException:
        config.load_kube_config()
        
    v1 = client.CoreV1Api()
    w = watch.Watch()
    
    logging.info(f"🚀 Starting {SCHEDULER_NAME} Engine - Ready to route pods...")
    
    for event in w.stream(v1.list_pod_for_all_namespaces):
        if event['type'] == 'ADDED':
            pod = event['object']
            
            if pod.status.phase == 'Pending' and pod.spec.scheduler_name == SCHEDULER_NAME:
                pod_name = pod.metadata.name
                namespace = pod.metadata.namespace
                annotations = pod.metadata.annotations or {}
                
                logging.info(f"--- 🔍 Intercepted pending pod: {pod_name} ---")
                
                nodes_dict = get_available_nodes(v1)
                if not nodes_dict:
                    logging.error("❌ No ready worker nodes found!")
                    continue
                
                best_node = calculate_best_node(nodes_dict, annotations)
                bind_pod(v1, pod_name, namespace, best_node)

if __name__ == '__main__':
    main()
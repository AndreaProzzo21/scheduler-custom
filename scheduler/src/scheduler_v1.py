"""
iot-twin-scheduler
==================
Custom Kubernetes scheduler per ambienti IoT / Digital Twin.

Versione "quasi prod-ready" per PoC di tesi.

Rispetto alla versione base sono stati risolti i seguenti problemi:

  1. Il watch di Kubernetes ora sopravvive a disconnessioni, timeout e "410 Gone"
     (retry con backoff + gestione del resource_version).
  2. Un'eccezione imprevista nella gestione di un singolo pod non fa più crashare
     l'intero processo: viene loggata e lo scheduler continua a lavorare.
  3. Il bind del pod viene verificato realmente (GET del pod) invece di assumere
     "successo" ogni volta che il client Python solleva il ValueError noto.
  4. Il fallback estremo (nessun dato utilizzabile) fa un vero round-robin tra i
     nodi invece di scegliere sempre lo stesso nodo.
  5. RESOURCE AWARENESS (novità): CPU e RAM vengono SEMPRE controllate, anche
     quando il pod chiede una strategia diversa (latenza/temperatura/disk-io):
       a) Hard filter di sicurezza: un nodo sopra soglia critica di CPU/RAM
          viene escluso a priori dai candidati, indipendentemente dal punteggio
          della strategia richiesta.
       b) Scoring pesato: tra i nodi "sicuri" rimasti, la metrica di strategia
          viene combinata con l'utilizzo % di CPU e RAM tramite pesi
          configurabili (env var), non solo con la metrica singola.
       c) Se il pod non specifica alcuna strategia ('default'), il peso della
          strategia è automaticamente 0 e lo scoring diventa puro CPU+RAM.
  6. Timeout di Prometheus configurabile e alzato di default (rete edge/IoT
     spesso lenta).
  7. Regex disk-io-aware estesa a mmcblk* (schede SD/eMMC, tipiche su Raspberry Pi/SBC).
  8. Tracking dei fallback consecutivi per strategia, con log ERROR se una
     strategia è strutturalmente rotta (non un blip temporaneo).
  9. Graceful shutdown su SIGTERM/SIGINT.

NON risolti volutamente (fuori scope per una PoC di tesi, da citare eventualmente
come limiti del lavoro in sede di discussione):
  - Uso delle resources.requests/limits DICHIARATE nel pod come nello scheduler
    nativo (qui si usa l'utilizzo REALE via Metrics Server come proxy della
    capacità residua, scelta più adatta a workload IoT variabili ma diversa
    dal comportamento standard di kube-scheduler).
  - Rispetto di nodeSelector / nodeAffinity / tolerations standard di Kubernetes.
  - Leader election per l'alta affidabilità (più repliche dello scheduler).
"""

from kubernetes import client, config, watch
import logging
import requests
import os
import math
import time
import signal
import itertools

# --- CONFIGURAZIONE GENERALE ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
SCHEDULER_NAME = "iot-twin-scheduler"
PROMETHEUS_URL = os.getenv("PROMETHEUS_URL", "http://prometheus-operated.monitoring.svc.cluster.local:9090")
PROMETHEUS_TIMEOUT = float(os.getenv("PROMETHEUS_TIMEOUT_SECONDS", "6"))
WATCH_ERROR_BACKOFF_SECONDS = float(os.getenv("WATCH_ERROR_BACKOFF_SECONDS", "5"))
WATCH_STREAM_TIMEOUT_SECONDS = int(os.getenv("WATCH_STREAM_TIMEOUT_SECONDS", "120"))
MAX_CONSECUTIVE_FALLBACKS_WARNING = int(os.getenv("MAX_CONSECUTIVE_FALLBACKS_WARNING", "5"))

# --- CONFIGURAZIONE RESOURCE AWARENESS (CPU/RAM) ---
# Soglie oltre le quali un nodo viene escluso A PRESCINDERE dalla strategia richiesta.
MAX_CPU_USAGE_PERCENT = float(os.getenv("MAX_CPU_USAGE_PERCENT", "85"))
MAX_MEM_USAGE_PERCENT = float(os.getenv("MAX_MEM_USAGE_PERCENT", "85"))
# Pesi per lo scoring combinato (non serve che sommino a 1: vengono rinormalizzati).
WEIGHT_STRATEGY = float(os.getenv("WEIGHT_STRATEGY", "0.6"))
WEIGHT_CPU = float(os.getenv("WEIGHT_CPU", "0.2"))
WEIGHT_MEM = float(os.getenv("WEIGHT_MEM", "0.2"))

# --- STATO GLOBALE MINIMO ---
_round_robin_cycle_cache = {}       # {frozenset(nodi): itertools.cycle}
_consecutive_fallback_count = {}    # {strategia: numero di fallback di fila}
_shutdown_requested = False


def _handle_shutdown_signal(signum, frame):
    global _shutdown_requested
    logging.info(f"🛑 Ricevuto segnale {signum}, chiusura in corso dopo l'evento corrente...")
    _shutdown_requested = True


signal.signal(signal.SIGTERM, _handle_shutdown_signal)
signal.signal(signal.SIGINT, _handle_shutdown_signal)


# --- KUBERNETES ACTIONS ---
def _verify_bind_result(api_instance, pod_name, namespace, node_name, retries=3, delay=1.0):
    """
    Verifica realmente (via GET) che il pod sia stato assegnato al nodo atteso,
    invece di fidarsi ciecamente del non-sollevamento di eccezione.
    """
    for attempt in range(1, retries + 1):
        try:
            pod = api_instance.read_namespaced_pod(name=pod_name, namespace=namespace)
            if pod.spec.node_name == node_name:
                logging.info(f"✅ Verificato: POD [{pod_name}] correttamente bindato a [{node_name}]")
                return True
            elif pod.spec.node_name:
                logging.error(
                    f"❌ POD [{pod_name}] risulta bindato a un nodo diverso da quello atteso "
                    f"(atteso: {node_name}, trovato: {pod.spec.node_name})"
                )
                return False
        except client.ApiException as e:
            logging.warning(f"⚠️ Verifica bind {pod_name} fallita (tentativo {attempt}/{retries}): {e}")
        time.sleep(delay)

    logging.error(f"❌ Verifica bind fallita per POD [{pod_name}]: nodeName non impostato dopo {retries} tentativi")
    return False


def bind_pod(api_instance, pod_name, namespace, node_name):
    target = client.V1ObjectReference(api_version='v1', kind='Node', name=node_name)
    meta = client.V1ObjectMeta(name=pod_name)
    binding = client.V1Binding(target=target, metadata=meta)

    try:
        api_instance.create_namespaced_pod_binding(name=pod_name, namespace=namespace, body=binding)
        logging.info(f"✅ Successfully bound POD [{pod_name}] to NODE [{node_name}]")
        return True
    except ValueError:
        # Bug noto della libreria Python di K8s: la risposta del binding a volte non
        # viene deserializzata correttamente anche se il binding sul server è riuscito.
        # Non assumiamo più il successo alla cieca: verifichiamo lo stato reale del pod.
        logging.warning(
            f"⚠️ ValueError durante il binding di {pod_name} (probabile bug noto del client "
            f"Python di K8s). Verifico lo stato reale con una GET..."
        )
        return _verify_bind_result(api_instance, pod_name, namespace, node_name)
    except client.ApiException as e:
        logging.error(f"❌ Failed to bind pod {pod_name}: {e}")
        return False


def get_available_nodes(api_instance):
    """
    Restituisce un dizionario { 'nome_nodo': 'indirizzo_ip' } con i soli nodi
    Ready e privi di taint NoSchedule (protegge il control-plane).
    """
    nodes = api_instance.list_node()
    valid_nodes = {}

    for n in nodes.items:
        conditions = n.status.conditions or []
        is_ready = any(c.type == 'Ready' and c.status == 'True' for c in conditions)

        is_schedulable = True
        if n.spec.taints:
            for taint in n.spec.taints:
                if taint.effect == 'NoSchedule':
                    is_schedulable = False
                    break

        if is_ready and is_schedulable:
            addresses = n.status.addresses or []
            internal_ip = next((addr.address for addr in addresses if addr.type == 'InternalIP'), None)
            if internal_ip:
                valid_nodes[n.metadata.name] = internal_ip

    return valid_nodes


# --- PROMETHEUS LOGIC (metriche di strategia) ---
def get_prometheus_metric(query):
    try:
        response = requests.get(
            f"{PROMETHEUS_URL}/api/v1/query",
            params={'query': query},
            timeout=PROMETHEUS_TIMEOUT
        )
        response.raise_for_status()
        results = response.json().get('data', {}).get('result', [])

        node_metrics = {}
        for res in results:
            labels = res.get('metric', {})
            raw_target = labels.get('kubernetes_node') or labels.get('instance', '')

            if raw_target:
                clean_target = raw_target.split(':')[0]
                node_metrics[clean_target] = float(res['value'][1])

        return node_metrics
    except Exception as e:
        logging.warning(f"⚠️ Prometheus fetch failed or timeout: {e}")
        return None


# --- RESOURCE AWARENESS (CPU/RAM via Metrics Server) ---
def _parse_cpu_to_millicores(cpu_str):
    """Converte una stringa CPU K8s ('250m', '2', '500000n', '10u') in millicores."""
    if cpu_str is None:
        return None
    cpu_str = str(cpu_str)
    try:
        if cpu_str.endswith('n'):
            return float(cpu_str[:-1]) / 1_000_000.0
        if cpu_str.endswith('u'):
            return float(cpu_str[:-1]) / 1_000.0
        if cpu_str.endswith('m'):
            return float(cpu_str[:-1])
        return float(cpu_str) * 1000.0
    except ValueError:
        return None


def _parse_memory_to_ki(mem_str):
    """Converte una stringa di memoria K8s ('3947172Ki', '4Gi', '512Mi', ...) in KiB."""
    if mem_str is None:
        return None
    mem_str = str(mem_str)
    units = {
        'Ki': 1, 'Mi': 1024, 'Gi': 1024 ** 2, 'Ti': 1024 ** 3,
        'K': 1, 'M': 1000, 'G': 1000 ** 2, 'T': 1000 ** 3,
    }
    try:
        for suffix, multiplier in sorted(units.items(), key=lambda x: -len(x[0])):
            if mem_str.endswith(suffix):
                return float(mem_str[:-len(suffix)]) * multiplier
        return float(mem_str) / 1024.0  # nessun suffisso -> assume bytes
    except ValueError:
        return None


def get_node_usage(nodes_dict):
    """
    Uso corrente di CPU/RAM per nodo secondo il Metrics Server.
    Ritorna { node_name: {'cpu_millicores': float, 'mem_ki': float} } oppure
    None se il Metrics Server non è raggiungibile.
    """
    try:
        custom_api = client.CustomObjectsApi()
        metrics = custom_api.list_cluster_custom_object("metrics.k8s.io", "v1beta1", "nodes")
        usage = {}
        for item in metrics.get('items', []):
            name = item['metadata']['name']
            if name not in nodes_dict:
                continue
            cpu_mc = _parse_cpu_to_millicores(item['usage'].get('cpu'))
            mem_ki = _parse_memory_to_ki(item['usage'].get('memory'))
            if cpu_mc is not None and mem_ki is not None:
                usage[name] = {'cpu_millicores': cpu_mc, 'mem_ki': mem_ki}
        return usage if usage else None
    except Exception as e:
        logging.warning(f"⚠️ Metrics Server non raggiungibile per il calcolo CPU/RAM: {e}")
        return None


def get_node_capacity(api_instance, nodes_dict):
    """
    Capacità ALLOCABILE (non totale) di CPU/RAM per nodo, per calcolare le
    percentuali di utilizzo reali. Ritorna { node_name: {'cpu_millicores', 'mem_ki'} }.
    """
    capacity = {}
    try:
        nodes = api_instance.list_node()
        for n in nodes.items:
            name = n.metadata.name
            if name not in nodes_dict:
                continue
            allocatable = n.status.allocatable or {}
            cpu_mc = _parse_cpu_to_millicores(allocatable.get('cpu'))
            mem_ki = _parse_memory_to_ki(allocatable.get('memory'))
            if cpu_mc and mem_ki:
                capacity[name] = {'cpu_millicores': cpu_mc, 'mem_ki': mem_ki}
    except Exception as e:
        logging.warning(f"⚠️ Impossibile leggere la capacità allocabile dei nodi: {e}")
    return capacity


def get_resource_usage_percent(api_instance, nodes_dict):
    """
    Percentuale di utilizzo CPU/RAM per nodo (usage / allocatable * 100).
    Ritorna {} se Metrics Server non è disponibile: in quel caso i controlli
    di risorsa vengono semplicemente SALTATI (non bloccano lo scheduling),
    così l'assenza del Metrics Server degrada in modo controllato invece di
    rompere tutto.
    """
    usage = get_node_usage(nodes_dict)
    if not usage:
        return {}

    capacity = get_node_capacity(api_instance, nodes_dict)
    if not capacity:
        return {}

    resource_pct = {}
    for node_name in nodes_dict:
        if node_name in usage and node_name in capacity:
            cpu_pct = (usage[node_name]['cpu_millicores'] / capacity[node_name]['cpu_millicores']) * 100
            mem_pct = (usage[node_name]['mem_ki'] / capacity[node_name]['mem_ki']) * 100
            resource_pct[node_name] = {'cpu_pct': cpu_pct, 'mem_pct': mem_pct}

    return resource_pct


def _normalize(values_dict):
    """Min-max normalization in [0,1]. Se tutti i valori sono uguali, ritorna 0 per tutti."""
    vals = list(values_dict.values())
    lo, hi = min(vals), max(vals)
    if hi == lo:
        return {k: 0.0 for k in values_dict}
    return {k: (v - lo) / (hi - lo) for k, v in values_dict.items()}


# --- FALLBACK LOGIC ---
def _get_round_robin_node(nodes_dict):
    """
    Vero round-robin: ruota tra i nodi disponibili invece di scegliere sempre
    lo stesso nodo (era il bug della versione precedente).
    """
    key = frozenset(nodes_dict.keys())
    if key not in _round_robin_cycle_cache:
        _round_robin_cycle_cache.clear()  # evita di accumulare cicli per set di nodi ormai obsoleti
        _round_robin_cycle_cache[key] = itertools.cycle(sorted(nodes_dict.keys()))
    return next(_round_robin_cycle_cache[key])


def fallback_metrics_server(nodes_dict):
    """
    Fallback quando non c'è nessuna metrica di strategia utilizzabile:
    sceglie il nodo con il minor carico CPU+RAM combinato (50/50) secondo il
    Metrics Server. Se anche questo non è disponibile, vero round-robin.
    """
    logging.info("🔄 Initiating Fallback: Querying K8s Metrics Server (CPU+RAM)...")
    usage = get_node_usage(nodes_dict)

    if usage:
        cpu_vals = {n: v['cpu_millicores'] for n, v in usage.items()}
        mem_vals = {n: v['mem_ki'] for n, v in usage.items()}

        norm_cpu = _normalize(cpu_vals)
        norm_mem = _normalize(mem_vals)
        combined = {n: 0.5 * norm_cpu.get(n, 0) + 0.5 * norm_mem.get(n, 0) for n in usage}

        best_node = min(combined, key=combined.get)
        logging.info(f"🔄 Fallback SUCCESS: nodo con minor carico CPU+RAM combinato -> {best_node}")
        return best_node

    logging.warning("⚠️ Metrics Server non disponibile o senza dati utili.")
    fallback_node = _get_round_robin_node(nodes_dict)
    logging.info(f"🆘 Fallback finale (Round-Robin reale): nodo selezionato -> {fallback_node}")
    return fallback_node


# --- SCHEDULING ENGINE ---
def _track_fallback(strategy):
    count = _consecutive_fallback_count.get(strategy, 0) + 1
    _consecutive_fallback_count[strategy] = count
    if count >= MAX_CONSECUTIVE_FALLBACKS_WARNING and count % MAX_CONSECUTIVE_FALLBACKS_WARNING == 0:
        logging.error(
            f"🚨 La strategia [{strategy}] è finita in fallback {count} volte di fila. "
            f"Probabile problema strutturale (query Prometheus/exporter mancante), non un blip temporaneo."
        )


def calculate_best_node(api_instance, nodes_dict, annotations):
    """
    Sceglie il nodo migliore combinando:
      - la metrica di strategia richiesta dal pod (latenza/temperatura/disk-io),
        se disponibile;
      - l'utilizzo % di CPU e RAM del nodo, SEMPRE calcolato, indipendentemente
        dalla strategia.

    Prima di tutto applica un hard filter di sicurezza: un nodo sopra soglia
    critica di CPU/RAM viene escluso a priori, qualunque sia il suo punteggio
    sulla strategia richiesta.
    """
    strategy = annotations.get('iot-scheduler/strategy', 'default')
    logging.info(f"🎯 Pod requested strategy: [{strategy}]")

    # --- 1. Metrica specifica della strategia (se applicabile) ---
    strategy_metrics = None
    if strategy == 'minimize-latency':
        strategy_metrics = get_prometheus_metric('avg(probe_duration_seconds) by (kubernetes_node, instance)')
    elif strategy == 'temperature-aware':
        strategy_metrics = get_prometheus_metric('avg(node_hwmon_temp_celsius) by (instance)')
    elif strategy == 'disk-io-aware':
        # mmcblk.* copre storage eMMC/SD, tipico su SBC IoT (es. Raspberry Pi)
        strategy_metrics = get_prometheus_metric(
            'sum(rate(node_disk_io_time_seconds_total{device=~"sd.*|vd.*|nvme.*|mmcblk.*"}[2m])) by (instance)'
        )

    strategy_available = bool(strategy_metrics)

    # --- 2. Utilizzo CPU/RAM: SEMPRE calcolato, qualunque sia la strategia ---
    resource_pct = get_resource_usage_percent(api_instance, nodes_dict)  # {} se Metrics Server assente

    # --- 3. HARD FILTER di sicurezza: escludi nodi sovraccarichi, se abbiamo i dati ---
    candidate_nodes = dict(nodes_dict)
    if resource_pct:
        safe_nodes = {
            n: ip for n, ip in nodes_dict.items()
            if resource_pct.get(n, {}).get('cpu_pct', 0) < MAX_CPU_USAGE_PERCENT
            and resource_pct.get(n, {}).get('mem_pct', 0) < MAX_MEM_USAGE_PERCENT
        }
        if safe_nodes:
            excluded = set(nodes_dict) - set(safe_nodes)
            if excluded:
                logging.info(
                    f"🛡️ Esclusi per sovraccarico CPU/RAM (soglia {MAX_CPU_USAGE_PERCENT}%/{MAX_MEM_USAGE_PERCENT}%): "
                    f"{sorted(excluded)}"
                )
            candidate_nodes = safe_nodes
        else:
            logging.warning(
                "⚠️ TUTTI i nodi superano la soglia di sicurezza CPU/RAM. "
                "Ignoro il filtro per questa volta per non lasciare il pod Pending indefinitamente."
            )

    if not strategy_available and not resource_pct:
        logging.warning(f"⚠️ Strategy [{strategy}] e metriche di risorsa entrambe non disponibili.")
        _track_fallback(strategy)
        return fallback_metrics_server(candidate_nodes)

    # --- 4. SCORING PESATO: combina metrica di strategia + CPU + RAM ---
    strategy_raw = {}
    if strategy_available:
        for node_name, node_ip in candidate_nodes.items():
            val = strategy_metrics.get(node_name, strategy_metrics.get(node_ip))
            if val is not None:
                strategy_raw[node_name] = val

    cpu_raw = {n: resource_pct[n]['cpu_pct'] for n in candidate_nodes if n in resource_pct}
    mem_raw = {n: resource_pct[n]['mem_pct'] for n in candidate_nodes if n in resource_pct}

    norm_strategy = _normalize(strategy_raw) if strategy_raw else {}
    norm_cpu = _normalize(cpu_raw) if cpu_raw else {}
    norm_mem = _normalize(mem_raw) if mem_raw else {}

    # Se una componente non ha dati, il suo peso viene azzerato e gli altri si
    # riprendono la quota (rinormalizzazione). Questo è ciò che fa sì che, senza
    # annotation (strategy='default'), lo score diventi puro CPU+RAM.
    w_strategy = WEIGHT_STRATEGY if norm_strategy else 0.0
    w_cpu = WEIGHT_CPU if norm_cpu else 0.0
    w_mem = WEIGHT_MEM if norm_mem else 0.0
    total_w = w_strategy + w_cpu + w_mem

    if total_w == 0:
        logging.warning(f"⚠️ Nessuna metrica utilizzabile per lo scoring di [{strategy}].")
        _track_fallback(strategy)
        return fallback_metrics_server(candidate_nodes)

    scores = {}
    for node_name in candidate_nodes:
        s = norm_strategy.get(node_name, 0.0) * w_strategy
        s += norm_cpu.get(node_name, 0.0) * w_cpu
        s += norm_mem.get(node_name, 0.0) * w_mem
        scores[node_name] = s / total_w
        logging.info(
            f"🧮 Node [{node_name}] - score={scores[node_name]:.3f} "
            f"(strategy_raw={strategy_raw.get(node_name)}, "
            f"cpu%={cpu_raw.get(node_name)}, mem%={mem_raw.get(node_name)})"
        )

    best_node = min(scores, key=scores.get)
    logging.info(f"🏆 Strategy [{strategy}] selected WINNER node: {best_node} (score={scores[best_node]:.3f})")
    _consecutive_fallback_count[strategy] = 0
    return best_node


def _process_pod_event(v1, pod):
    # 1. Avvio cronometri
    start_time_perf = time.perf_counter() # Alta precisione per la durata (overhead)
    start_time_sys = time.time()          # Timestamp assoluto (Epoch) per il recovery time
    
    pod_name = pod.metadata.name
    namespace = pod.metadata.namespace
    annotations = pod.metadata.annotations or {}

    logging.info(f"--- 🔍 Intercepted pending pod: {pod_name} ---")
    # Log di inizio con timestamp
    logging.info(f"📊 [METRIC-START] Pod: {pod_name} | Intercepted_At: {start_time_sys}")

    nodes_dict = get_available_nodes(v1)
    if not nodes_dict:
        logging.error("❌ No ready worker nodes found!")
        return

    best_node = calculate_best_node(v1, nodes_dict, annotations)
    if not best_node:
        logging.error(
            f"❌ Nessun nodo selezionabile per il pod {pod_name}: rimarrà Pending, "
            f"riproverò al prossimo giro."
        )
        return

    # Esegue il binding (salviamo l'esito visto che ora bind_pod restituisce True/False)
    bind_success = bind_pod(v1, pod_name, namespace, best_node)

    # 2. Fine cronometro e calcolo
    end_time_perf = time.perf_counter()
    duration_ms = (end_time_perf - start_time_perf) * 1000

    if bind_success:
        # Log finale per i grafici
        logging.info(
            f"📊 [METRIC-END] Pod: {pod_name} | TargetNode: {best_node} | "
            f"Overhead: {duration_ms:.2f} ms"
        )


# --- MAIN LOOP ---
def main():
    try:
        config.load_incluster_config()
    except config.ConfigException:
        config.load_kube_config()

    v1 = client.CoreV1Api()

    logging.info(f"🚀 Starting {SCHEDULER_NAME} Engine - Ready to route pods...")

    resource_version = ''

    while not _shutdown_requested:
        w = watch.Watch()
        try:
            stream_kwargs = {'timeout_seconds': WATCH_STREAM_TIMEOUT_SECONDS}
            if resource_version:
                stream_kwargs['resource_version'] = resource_version

            for event in w.stream(v1.list_pod_for_all_namespaces, **stream_kwargs):
                if _shutdown_requested:
                    w.stop()
                    break

                try:
                    resource_version = event['object'].metadata.resource_version
                except Exception:
                    pass

                if event['type'] == 'ADDED':
                    pod = event['object']

                    if pod.status.phase == 'Pending' and pod.spec.scheduler_name == SCHEDULER_NAME:
                        try:
                            _process_pod_event(v1, pod)
                        except Exception as e:
                            logging.error(
                                f"❌ Errore imprevisto processando il pod {pod.metadata.name}: {e}",
                                exc_info=True
                            )

        except client.ApiException as e:
            if e.status == 410:
                logging.warning("⚠️ Watch scaduto (410 Gone): riparto con un resource_version fresco...")
                resource_version = ''
            else:
                logging.error(f"❌ Errore API durante il watch: {e}. Riprovo tra {WATCH_ERROR_BACKOFF_SECONDS}s...")
                time.sleep(WATCH_ERROR_BACKOFF_SECONDS)
        except Exception as e:
            logging.error(
                f"❌ Errore imprevisto nel watch loop: {e}. Riprovo tra {WATCH_ERROR_BACKOFF_SECONDS}s...",
                exc_info=True
            )
            time.sleep(WATCH_ERROR_BACKOFF_SECONDS)
        finally:
            w.stop()

    logging.info("👋 Scheduler terminato correttamente (graceful shutdown).")


if __name__ == '__main__':
    main()
"""
=============================================================================
 IoT-Twin-Scheduler: Academic Chart Generator for Kubernetes Digital Twins
=============================================================================
 Genera 6 grafici pronti per la pubblicazione:
 1. Latenza Istantanea (Bar Chart)
 2. Latenza Temporale (Line Chart)
 3. Utilizzo CPU Temporale (Line Chart)
 4. Utilizzo RAM Temporale (Line Chart)
 5a. Pod Placement Baseline Scheduler (Bar Chart)
 5b. Pod Placement Custom MCDM Scheduler (Bar Chart)
"""

import requests
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import matplotlib.ticker as mticker
import datetime
import time
from kubernetes import client, config

# ===========================================================================
# CONFIGURAZIONE STILE ACCADEMICO E TARGET
# ===========================================================================
plt.rcParams.update({
    'font.family': 'serif',
    'font.size': 12,
    'axes.labelsize': 14,
    'axes.titlesize': 14,
    'axes.titleweight': 'bold',
    'xtick.labelsize': 12,
    'ytick.labelsize': 12,
    'legend.fontsize': 12,
    'figure.autolayout': True,
    'axes.grid': True,
    'grid.alpha': 0.6,
    'grid.linestyle': ':',
    'grid.color': '#999999'
})

PROMETHEUS_URL = "http://127.0.0.1:9090"
WINDOW_SECONDS = 180
STEP_SECONDS = 5

def get_prom_data(query, is_range=False, start=None, end=None, step=None):
    """Esegue query verso Prometheus."""
    endpoint = f"{PROMETHEUS_URL}/api/v1/query_range" if is_range else f"{PROMETHEUS_URL}/api/v1/query"
    params = {'query': query}
    if is_range:
        params.update({'start': start, 'end': end, 'step': step})
    
    try:
        response = requests.get(endpoint, params=params)
        response.raise_for_status()
        return response.json().get('data', {}).get('result', [])
    except Exception as e:
        print(f"[-] Errore Prometheus: {e}")
        return []

def clean_node_name(raw_name):
    """Pulisce il nome del nodo."""
    if not raw_name:
        return "Unknown"
    clean = raw_name.split(':')[0].replace('talos-tim-', '').capitalize()
    return clean

# ===========================================================================
# FUNZIONI DI PLOTTING
# ===========================================================================

def plot_bar_chart(nodes, values, ylabel, title, filename):
    if not nodes or not values:
        print(f"[-] Dati vuoti per {filename}. Salto la generazione.")
        return

    fig, ax = plt.subplots(figsize=(8, 5))
    colors = ['#e0e0e0', '#a0a0a0', '#505050'] 
    hatches = ['//', '\\\\', 'xx']

    bars = ax.bar(nodes, values, color=colors[:len(nodes)], edgecolor='black', zorder=3, width=0.6)
    
    for bar, hatch in zip(bars, hatches[:len(nodes)]):
        bar.set_hatch(hatch)

    ax.set_ylabel(ylabel)
    ax.set_xlabel('Edge Nodes')
    ax.set_title(title)
    
    for i, v in enumerate(values):
        val_str = f'{v:.1f}' if v < 10 else f'{int(v)}'
        ax.text(i, v + (max(values)*0.02), f'{val_str}', ha='center', va='bottom', fontweight='bold')

    ax.set_ylim(0, max(values) * 1.20)
    plt.savefig(filename, dpi=300)
    print(f"[+] Salvato: {filename}")
    plt.close()

def plot_line_chart(results, ylabel, title, filename, multiplier=1.0, is_percent=False):
    if not results:
        print(f"[-] Nessun dato per generare: {filename}. Salto.")
        return

    fig, ax = plt.subplots(figsize=(9, 5))
    line_styles = ['-', '--', '-.']
    markers = ['o', 's', '^']
    colors = ['black', '#444444', '#777777']
    
    results_sorted = sorted(results, key=lambda x: clean_node_name(x['metric'].get('kubernetes_node', x['metric'].get('node', ''))))

    plotted_lines = 0
    for idx, res in enumerate(results_sorted):
        raw_node = res['metric'].get('kubernetes_node', res['metric'].get('node', res['metric'].get('instance', '')))
        nodo = clean_node_name(raw_node)
        
        raw_vals = str(res['metric'].values()).lower()
        if nodo == "Unknown" or nodo == "Localhost" or "192.168.0.61" in raw_vals or "controlplane" in nodo.lower(): 
            continue
            
        times = [datetime.datetime.fromtimestamp(float(v[0])) for v in res['values']]
        values = [float(v[1]) * multiplier for v in res['values']]
        
        if not values:
            continue
            
        style_idx = plotted_lines % len(line_styles)
        ax.plot(times, values, label=nodo, 
                linestyle=line_styles[style_idx], 
                marker=markers[style_idx], 
                color=colors[style_idx],
                markersize=5,
                linewidth=1.8,
                markevery=max(1, len(times)//15))
        plotted_lines += 1
        
    if plotted_lines == 0:
        print(f"[-] Nessuna linea tracciata per {filename}. Salto.")
        plt.close()
        return

    ax.set_ylabel(ylabel)
    ax.set_xlabel('Time')
    ax.set_title(title)
    
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M:%S'))
    fig.autofmt_xdate()
    
    if is_percent:
        ax.set_ylim(0, 100)
        ax.yaxis.set_major_formatter(mticker.PercentFormatter())
    else:
        ylim_top = ax.get_ylim()[1]
        ax.set_ylim(0, ylim_top * 1.1)

    ax.legend(loc='best', framealpha=0.9, edgecolor='black')
    plt.savefig(filename, dpi=300)
    print(f"[+] Salvato: {filename}")
    plt.close()

def plot_pod_distribution(node_counts, title, filename):
    if not node_counts:
        print(f"[-] Dati Pod vuoti per {filename}. Salto.")
        return
        
    nodes = sorted(list(node_counts.keys()))
    values = [node_counts[n] for n in nodes]
    
    fig, ax = plt.subplots(figsize=(7, 5))
    colors = ['#505050', '#a0a0a0', '#e0e0e0']
    
    bars = ax.bar(nodes, values, color=colors[:len(nodes)], edgecolor='black', width=0.5, zorder=3)
    
    ax.set_ylabel('Number of Scheduled Pods')
    ax.set_xlabel('Edge Nodes')
    ax.set_title(title)
    
    ax.yaxis.set_major_locator(mticker.MaxNLocator(integer=True))
    
    for i, v in enumerate(values):
        ax.text(i, v + 0.1, str(int(v)), ha='center', va='bottom', fontweight='bold')
        
    max_val = max(values) if values else 1
    ax.set_ylim(0, max(max_val * 1.3, 1.5)) 
    
    plt.savefig(filename, dpi=300)
    print(f"[+] Salvato: {filename}")
    plt.close()

# ===========================================================================
# ESECUZIONE MAIN
# ===========================================================================
if __name__ == "__main__":
    print("[*] Generazione Suite Grafici Accademici per Tesi...")
    
    end_time = int(time.time())
    start_time = end_time - WINDOW_SECONDS
    
    known_workers = set() 

    # 1. LATENZA ISTANTANEA
    query_lat = 'probe_duration_seconds'
    inst_lat = get_prom_data(query_lat)
    if inst_lat:
        nodi, latenze = [], []
        for res in sorted(inst_lat, key=lambda x: clean_node_name(x['metric'].get('kubernetes_node', ''))):
            raw_vals = str(res['metric'].values()).lower()
            nodo_clean = clean_node_name(res['metric'].get('kubernetes_node', ''))
            if "192.168.0.61" in raw_vals or "controlplane" in nodo_clean.lower() or "localhost" in nodo_clean.lower():
                continue
            nodi.append(nodo_clean)
            latenze.append(float(res['value'][1]) * 1000)
            known_workers.add(nodo_clean)
        plot_bar_chart(nodi, latenze, 'Latency (ms)', f'Instant OT Network Latency', 'fig_01_latency_bar.png')

    # 2. LATENZA TEMPORALE
    range_lat = get_prom_data(query_lat, is_range=True, start=start_time, end=end_time, step=STEP_SECONDS)
    plot_line_chart(range_lat, 'Latency (ms)', 'OT Network Latency Trend', 'fig_02_latency_line.png', multiplier=1000)

    # 3. CPU TEMPORALE
    query_cpu = '100 - (avg by (node, kubernetes_node, instance) (irate(node_cpu_seconds_total{mode="idle"}[2m])) * 100)'
    range_cpu = get_prom_data(query_cpu, is_range=True, start=start_time, end=end_time, step=STEP_SECONDS)
    plot_line_chart(range_cpu, 'CPU Usage (%)', 'Edge Nodes CPU Utilization', 'fig_03_cpu_line.png', is_percent=True)

    # 4. RAM TEMPORALE
    query_ram = '100 * (1 - (avg by (node, kubernetes_node, instance) (node_memory_MemAvailable_bytes / node_memory_MemTotal_bytes)))'
    range_ram = get_prom_data(query_ram, is_range=True, start=start_time, end=end_time, step=STEP_SECONDS)
    plot_line_chart(range_ram, 'RAM Usage (%)', 'Edge Nodes Memory Utilization', 'fig_04_ram_line.png', is_percent=True)
    
    # =======================================================================
    # 5. POD PLACEMENT DISTRIBUTION (SEPARATO: BASELINE VS CUSTOM)
    # =======================================================================
    print("\n[*] Estrazione Dati Pod Placement (Baseline vs Custom) da Kubernetes API...")
    try:
        try:
            config.load_kube_config()
        except Exception:
            config.load_incluster_config()
            
        v1 = client.CoreV1Api()
        pods = v1.list_pod_for_all_namespaces(watch=False)
        
        # Assicuriamoci di includere tutti i worker noti nei dizionari finali
        base_counts = {w: 0 for w in known_workers}
        custom_counts = {w: 0 for w in known_workers}
        if not base_counts:
            base_counts = {"Worker-1": 0, "Worker-2": 0, "Worker-3": 0}
            custom_counts = {"Worker-1": 0, "Worker-2": 0, "Worker-3": 0}

        base_found = 0
        custom_found = 0

        for p in pods.items:
            pod_name = p.metadata.name
            node_name = p.spec.node_name
            
            if not node_name:
                continue
                
            n_clean = clean_node_name(node_name)
            if "192.168.0.61" in node_name or "controlplane" in n_clean.lower():
                continue

            # Riconoscimento basato sui prefissi dei tuoi Deployment
            if "dt-baseline-default" in pod_name:
                if n_clean not in base_counts:
                    base_counts[n_clean] = 0
                base_counts[n_clean] += 1
                base_found += 1
                
            elif "dt-mcdm-custom" in pod_name:
                if n_clean not in custom_counts:
                    custom_counts[n_clean] = 0
                custom_counts[n_clean] += 1
                custom_found += 1
                    
        print(f"[+] Mappati {base_found} pod Baseline e {custom_found} pod Custom MCDM.")
        
        # Genera i due grafici separati
        plot_pod_distribution(base_counts, 'Default K8s Scheduler (Baseline)', 'fig_05a_pod_distribution_baseline.png')
        plot_pod_distribution(custom_counts, 'Custom MCDM Scheduler (IoT-Twin)', 'fig_05b_pod_distribution_custom.png')
        
    except Exception as e:
        print(f"[-] Impossibile interrogare l'API di Kubernetes per i pod: {e}")

    print("\n[✔] Operazione completata con successo!")
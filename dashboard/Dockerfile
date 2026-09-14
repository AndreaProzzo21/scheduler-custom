FROM python:3.9-slim

WORKDIR /app

RUN pip install --no-cache-dir flask kubernetes requests

COPY app.py index.html ./

# --- Runtime configuration (override at deploy time) ---------------------
# ENV PROMETHEUS_URL=http://prometheus-operated.monitoring.svc.cluster.local:9090
# ENV SCHEDULER_NAME=iot-twin-scheduler
# ENV SCHEDULER_DEPLOYMENT_NAME=iot-twin-scheduler
# ENV SCHEDULER_NAMESPACE=kube-system
# ENV SCHED_METRIC_DECISIONS=sum(iot_scheduler_scheduling_decisions_total)
# ENV SCHED_METRIC_ERRORS=sum(iot_scheduler_scheduling_errors_total)
# ENV SCHED_METRIC_LATENCY=avg(iot_scheduler_scheduling_duration_seconds) * 1000
# ENV SCHED_METRIC_QUEUE=sum(iot_scheduler_pending_queue_size)

EXPOSE 8080

CMD ["python", "app.py"]

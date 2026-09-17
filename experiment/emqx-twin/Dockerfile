FROM python:3.9-alpine

WORKDIR /app

# Installiamo la libreria MQTT
RUN pip install paho-mqtt

# Copiamo lo script
COPY mini_dt.py .

# Forza Python a stampare i log istantaneamente (non bufferizzati)
ENV PYTHONUNBUFFERED=1

CMD ["python", "mini_dt.py"]

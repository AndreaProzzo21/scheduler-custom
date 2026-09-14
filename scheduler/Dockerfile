FROM python:3.11-slim

WORKDIR /app

# Copia e installa le dipendenze
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copia il codice sorgente
COPY src/ .

# Avvia lo scheduler
CMD ["python", "scheduler_v2.py"]

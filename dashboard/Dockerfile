FROM python:3.9-slim

WORKDIR /app

RUN pip install flask kubernetes requests

COPY app.py index.html ./

EXPOSE 8080

CMD ["python", "app.py"]

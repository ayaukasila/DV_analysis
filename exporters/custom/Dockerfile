FROM python:3.11-slim
WORKDIR /app
COPY custom_exporter.py .
RUN pip install prometheus_client requests
EXPOSE 8000
CMD ["python", "custom_exporter.py"]

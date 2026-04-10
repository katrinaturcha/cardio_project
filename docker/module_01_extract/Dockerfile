FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt /app/requirements.txt
RUN apt-get update && apt-get install -y --no-install-recommends chromium chromium-driver && rm -rf /var/lib/apt/lists/*
RUN pip install --no-cache-dir -r /app/requirements.txt
RUN pip install openpyxl
COPY . /app
ENV PYTHONPATH=/app
CMD ["python", "src/modules/01_extract_eis.py"]

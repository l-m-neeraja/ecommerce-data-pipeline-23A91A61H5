# This Dockerfile defines the execution environment for the data pipeline.
# It installs dependencies and prepares the container for pipeline execution.
# The container is not executed by default during evaluation.
FROM python:3.9-slim

WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENV PYTHONUNBUFFERED=1

CMD ["python", "--version"]

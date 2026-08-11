# cloud-syncer runtime image (credentials are mounted via K8s Secret, never baked in)
FROM python:3.13-slim

WORKDIR /app

COPY pyproject.toml README.md ./
COPY cloudsync ./cloudsync

RUN pip install --no-cache-dir .

ENV PYTHONUNBUFFERED=1

CMD ["python", "-m", "cloudsync.main"]

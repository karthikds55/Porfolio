FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Default: run the full local pipeline (ingest → transform → quality checks)
CMD ["bash", "-c", \
     "python -m pipelines.ingest && \
      python -m pipelines.transform && \
      python -m transforms.quality_checks"]

# Data Engineering Portfolio

[![CI](https://github.com/karthikds55/DataEngineering/actions/workflows/ci.yml/badge.svg)](https://github.com/karthikds55/DataEngineering/actions/workflows/ci.yml)
[![Python](https://img.shields.io/badge/Python-3.11-blue?logo=python&logoColor=white)](https://www.python.org/)
[![AWS](https://img.shields.io/badge/AWS-Lambda%20%7C%20S3-orange?logo=amazonaws&logoColor=white)](https://aws.amazon.com/)
[![Terraform](https://img.shields.io/badge/IaC-Terraform-7B42BC?logo=terraform&logoColor=white)](https://www.terraform.io/)
[![Airflow](https://img.shields.io/badge/Orchestration-Airflow-017CEE?logo=apacheairflow&logoColor=white)](https://airflow.apache.org/)

Production-style data engineering pipeline demonstrating end-to-end patterns: cloud ingestion, transformation, data quality, infrastructure-as-code, orchestration, and automated testing.

---

## Architecture

```
Any file lands in S3 (CSV / JSON / Excel / Parquet)
         │
         │  s3:ObjectCreated:*
         ▼
┌─────────────────────┐
│  AWS Lambda         │   s3_to_parquet.py
│  S3 → Parquet       │   Snappy compression
└────────┬────────────┘
         │
         ▼  S3 Parquet bucket
         │
         ├─── Cloud mode: transform.py reads directly from S3
         │
         ▼  Local mode (development)
┌─────────────────────┐
│  Ingest Pipeline    │   pipelines/ingest.py
│  Raw → Staging      │   Schema validation, dtype coercion
└────────┬────────────┘
         ▼
┌─────────────────────┐
│  Transform Pipeline │   pipelines/transform.py
│  Staging → Marts    │   Daily summary, category summary
└────────┬────────────┘
         ▼
┌─────────────────────┐
│  Quality Checks     │   transforms/quality_checks.py
│  Validate output    │   Nulls, duplicates, value ranges
└────────┬────────────┘
         ▼
┌─────────────────────┐
│  Airflow DAG        │   dags/ecommerce_pipeline_dag.py
│  Orchestration      │   Scheduled daily, retry logic
└─────────────────────┘
```

---

## Project Structure

```
DataEngineering/
├── .github/workflows/ci.yml          # GitHub Actions: pytest on every push
├── dags/
│   └── ecommerce_pipeline_dag.py     # Airflow DAG: ingest → transform → quality
├── data/
│   └── raw/
│       └── daily_ecommerce_orders.csv
├── notebooks/
│   └── ecommerce_data_profiling.ipynb
├── pipelines/
│   ├── ingest.py                     # Raw CSV → staging layer
│   ├── transform.py                  # Staging → daily/category mart tables
│   ├── s3_to_parquet.py              # Lambda handler: any file → Parquet
│   └── s3_utils.py                   # S3 download/upload helpers
├── scripts/
│   └── build_lambda.sh               # Packages Lambda code + deps → ZIP
├── terraform/
│   ├── main.tf                       # S3 buckets, Lambda, S3 event trigger
│   ├── iam.tf                        # Lambda execution role, S3 policy
│   ├── variables.tf
│   ├── outputs.tf
│   └── terraform.tfvars.example
├── tests/
│   ├── test_pipelines.py             # 13 tests: ingest, transform, quality
│   └── test_s3_to_parquet.py         # 21 tests: Lambda handler, all formats
├── transforms/
│   └── quality_checks.py             # Null, duplicate, range validators
├── .env.example
├── Dockerfile
├── Makefile
└── requirements.txt
```

---

## Quickstart — Local

```bash
git clone https://github.com/karthikds55/DataEngineering.git
cd DataEngineering

pip install -r requirements.txt

# Run the full pipeline
make pipeline

# Or step by step
make ingest      # raw CSV → data/staging/orders_staging.csv
make transform   # staging → data/marts/daily_summary.csv + category_summary.csv
make quality     # validate staging data (nulls, duplicates, ranges)

# Run all tests
make test
```

---

## Quickstart — Docker

```bash
make docker-build

# Mounts local data/ folder so output is accessible after the run
make docker-run
```

---

## Cloud Deployment — AWS Lambda + Terraform

The `s3_to_parquet` Lambda converts any file dropped into the raw S3 bucket to Parquet automatically. Supported formats: **CSV, JSON, JSONL, Excel (.xlsx/.xls), Parquet (passthrough)**.

### Deploy

```bash
# 1. Build Lambda deployment package
bash scripts/build_lambda.sh

# 2. Configure Terraform
cd terraform
cp terraform.tfvars.example terraform.tfvars
# Edit terraform.tfvars — set globally unique bucket names

# 3. Deploy infrastructure
terraform init
terraform plan
terraform apply
```

### What Terraform provisions

| Resource | Purpose |
|---|---|
| `aws_s3_bucket` (×2) | Separate raw input and Parquet output buckets |
| `aws_lambda_function` | S3-to-Parquet converter, 1 GB RAM, 5 min timeout |
| `aws_s3_bucket_notification` | Triggers Lambda on `s3:ObjectCreated:*` |
| `aws_iam_role` + `aws_iam_policy` | Least-privilege: GetObject on raw, PutObject on parquet |
| `aws_cloudwatch_log_group` | Lambda logs, 14-day retention |

### Run the cloud pipeline end-to-end

```bash
# Upload a file to trigger the Lambda
aws s3 cp data/raw/daily_ecommerce_orders.csv s3://YOUR-RAW-BUCKET/raw/

# Once Lambda converts it, run transform against S3 Parquet output
S3_STAGING_BUCKET=YOUR-PARQUET-BUCKET \
S3_STAGING_KEY=daily_ecommerce_orders.parquet \
python -m pipelines.transform
```

---

## Orchestration — Airflow DAG

The DAG `ecommerce_pipeline` models the full local pipeline as three sequential tasks with retry logic and daily scheduling.

```
ingest_orders → transform_orders → quality_checks
```

```bash
pip install apache-airflow
export AIRFLOW_HOME=~/airflow
airflow db init
airflow dags trigger ecommerce_pipeline
```

---

## Testing

34 tests, no real AWS credentials required — all S3 calls are mocked.

```bash
make test
# or
pytest tests/ -v
```

| Test file | Coverage |
|---|---|
| `test_pipelines.py` | Ingest schema validation, dtype coercion, transform aggregations, quality checks |
| `test_s3_to_parquet.py` | Lambda handler, all file formats, S3 event parsing, error capture |

---

## Tech Stack

| Layer | Technology |
|---|---|
| Language | Python 3.11 |
| Data processing | pandas, PyArrow |
| Cloud | AWS Lambda, S3, CloudWatch |
| IaC | Terraform |
| Orchestration | Apache Airflow |
| File formats | CSV, JSON, JSONL, Excel, Parquet |
| Testing | pytest (34 tests) |
| CI/CD | GitHub Actions |
| Containerization | Docker |

---

## Contact

<p>
  <a href="https://www.linkedin.com/in/karthikdarapaneni/">
    <img src="https://img.shields.io/badge/LinkedIn-Connect-0A66C2?style=for-the-badge&logo=linkedin&logoColor=white" />
  </a>
  <a href="mailto:darapuneni.karthik@gmail.com">
    <img src="https://img.shields.io/badge/Email-Contact-EA4335?style=for-the-badge&logo=gmail&logoColor=white" />
  </a>
  <a href="https://github.com/karthikds55">
    <img src="https://img.shields.io/badge/GitHub-Follow-181717?style=for-the-badge&logo=github&logoColor=white" />
  </a>
</p>

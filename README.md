<!-- Classic "background" banner (image) -->
<p align="center">
  <img src="https://capsule-render.vercel.app/api?type=rect&color=0:111827,100:374151&height=140&section=header&text=Karthik%20Darapaneni&fontColor=E5E7EB&fontSize=48&animation=fadeIn" />
</p>

<p align="center">
  <b>Cloud Data Engineer (5+ years)</b> • AWS / Azure / GCP • SQL • Python • Spark • Airflow
</p>
<p align="center">
  Reliable pipelines • Data Modeling • Data Quality • Observability • Cost/Performance
</p>

<!-- All hyperlinks as classic monochrome buttons -->
<p align="center">
  <a href="https://www.linkedin.com/in/karthikdarapaneni/?lipi=urn%3Ali%3Apage%3Ad_flagship3_profile_view_base_contact_details%3Brv%2FjQHUtQTWO2UmdIxbXOQ%3D%3D/">
    <img src="https://img.shields.io/badge/LinkedIn-Connect-111827?style=for-the-badge&logo=linkedin&logoColor=white" />
  </a>
  <a href="mailto:YOUR_EMAIL@example.com">
    <img src="https://img.shields.io/badge/Email-Contact-111827?style=for-the-badge&logo=gmail&logoColor=white" />
  </a>
  <a href="https://github.com/karthikds55">
    <img src="https://img.shields.io/badge/GitHub-Follow-111827?style=for-the-badge&logo=github&logoColor=white" />
  </a>
  <a href="https://YOUR_PORTFOLIO.com">
    <img src="https://img.shields.io/badge/Portfolio-Visit-111827?style=for-the-badge&logo=vercel&logoColor=white" />
  </a>
</p>

<hr/>

<!-- Classic navigation row -->
<p align="center">
  <a href="#featured-work"><img src="https://img.shields.io/badge/Featured%20Work-Open-374151?style=for-the-badge" /></a>
  <a href="#core-skills"><img src="https://img.shields.io/badge/Core%20Skills-View-374151?style=for-the-badge" /></a>
  <a href="#cloud--data-stack"><img src="https://img.shields.io/badge/Cloud%20%26%20Data%20Stack-Browse-374151?style=for-the-badge" /></a>
  <a href="#highlights"><img src="https://img.shields.io/badge/Highlights-See-374151?style=for-the-badge" /></a>
  <a href="#contact"><img src="https://img.shields.io/badge/Contact-Reach%20out-374151?style=for-the-badge" /></a>
</p>

---

## Quick Start — Git Bash Setup

Clone the repo and check out the `data-engineering` branch in Git Bash:

```bash
# 1. Clone the repository
git clone https://github.com/karthikds55/DataEngineering.git
cd DataEngineering

# 2. Fetch all remote branches
git fetch --all

# 3. Check out the data-engineering branch
git checkout data-engineering

# 4. Install Python dependencies
pip install -r requirements.txt

# 5. Run the full pipeline (ingest → transform → quality checks)
python -m pipelines.ingest
python -m pipelines.transform
python -m transforms.quality_checks

# 6. Run the test suite
pytest tests/
```

> **Already on the branch?**  Run `git branch` to confirm — you should see `* data-engineering`.

---

## Project Structure

```
DataEngineering/
├── data/
│   ├── raw/           # Source files (CSV, JSON, …)
│   ├── staging/       # Post-ingest, pre-transform layer
│   └── marts/         # Aggregated mart tables
├── notebooks/         # Exploratory & profiling notebooks
├── pipelines/
│   ├── ingest.py      # Raw → staging ingestion
│   └── transform.py   # Staging → mart transforms
├── transforms/
│   └── quality_checks.py  # Schema, null, range checks
├── tests/
│   └── test_pipelines.py  # pytest unit tests
├── reports/           # Generated HTML profiling reports
└── requirements.txt
```

---

## Featured Work

<p>
  <a href="https://github.com/karthikds55/DataEngineering">
    <img src="https://img.shields.io/badge/Repo-DataEngineering-111827?style=for-the-badge&logo=github&logoColor=white" />
  </a>
</p>

**DataEngineering** — a portfolio repo for production-style patterns:
- ingestion → transforms → tests → orchestration
- quality gates + monitoring mindset
- scalable structure for cloud deployments

<details>
  <summary><b>Roadmap</b></summary>

- Raw → staging → marts example pipeline
- Data quality checks (schema, freshness, duplicates)
- Orchestration DAG + backfills
- CI checks (lint/test)
</details>

---

## Core Skills
- **SQL (advanced):** modeling, performance tuning, validation
- **Python:** pipelines, automation, API ingestion
- **Orchestration:** Airflow / Prefect / Dagster
- **Transforms:** dbt / Spark / SQL
- **Modeling:** facts/dims, SCD, marts
- **Quality & Observability:** tests, SLAs, monitoring/alerting
- **Cost/Performance:** partitioning, clustering, Parquet, incremental loads

---

## Cloud & Data Stack
<details>
  <summary><b>AWS</b></summary>

<p>
  <a href="https://aws.amazon.com/s3/"><img src="https://img.shields.io/badge/S3-Open-111827?style=for-the-badge&logo=amazonaws&logoColor=white" /></a>
  <a href="https://aws.amazon.com/glue/"><img src="https://img.shields.io/badge/Glue-Open-111827?style=for-the-badge&logo=amazonaws&logoColor=white" /></a>
  <a href="https://aws.amazon.com/redshift/"><img src="https://img.shields.io/badge/Redshift-Open-111827?style=for-the-badge&logo=amazonaws&logoColor=white" /></a>
</p>
</details>

<details>
  <summary><b>Azure</b></summary>

<p>
  <a href="https://azure.microsoft.com/products/storage/data-lake-storage/"><img src="https://img.shields.io/badge/ADLS-Open-111827?style=for-the-badge&logo=microsoftazure&logoColor=white" /></a>
  <a href="https://azure.microsoft.com/products/data-factory/"><img src="https://img.shields.io/badge/Data%20Factory-Open-111827?style=for-the-badge&logo=microsoftazure&logoColor=white" /></a>
  <a href="https://azure.microsoft.com/products/synapse-analytics/"><img src="https://img.shields.io/badge/Synapse-Open-111827?style=for-the-badge&logo=microsoftazure&logoColor=white" /></a>
</p>
</details>

<details>
  <summary><b>GCP</b></summary>

<p>
  <a href="https://cloud.google.com/storage"><img src="https://img.shields.io/badge/GCS-Open-111827?style=for-the-badge&logo=googlecloud&logoColor=white" /></a>
  <a href="https://cloud.google.com/bigquery"><img src="https://img.shields.io/badge/BigQuery-Open-111827?style=for-the-badge&logo=googlecloud&logoColor=white" /></a>
  <a href="https://cloud.google.com/pubsub"><img src="https://img.shields.io/badge/Pub%2FSub-Open-111827?style=for-the-badge&logo=googlecloud&logoColor=white" /></a>
</p>
</details>

---

## Highlights
- 5+ years delivering cloud pipelines end-to-end (ingestion → curated marts)
- Strong emphasis on **data trust** (testing + observability) and **practical performance**

---

## Contact
<p align="center">
  <a href="https://www.linkedin.com/in/YOUR_LINKEDIN/">
    <img src="https://img.shields.io/badge/LinkedIn-Message-374151?style=for-the-badge&logo=linkedin&logoColor=white" />
  </a>
  <a href="mailto:YOUR_EMAIL@example.com">
    <img src="https://img.shields.io/badge/Email-Send-374151?style=for-the-badge&logo=gmail&logoColor=white" />
  </a>
  <a href="https://YOUR_PORTFOLIO.com">
    <img src="https://img.shields.io/badge/Portfolio-Open-374151?style=for-the-badge&logo=vercel&logoColor=white" />
  </a>
</p>

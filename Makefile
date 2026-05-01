.PHONY: install test ingest transform quality pipeline docker-build docker-run deploy

install:
	pip install -r requirements.txt

# ── Pipeline (local) ──────────────────────────────────────────────────────────

ingest:
	python -m pipelines.ingest

transform:
	python -m pipelines.transform

quality:
	python -m transforms.quality_checks

pipeline: ingest transform quality

# ── Tests ─────────────────────────────────────────────────────────────────────

test:
	pytest tests/ -v --tb=short

# ── Docker ────────────────────────────────────────────────────────────────────

docker-build:
	docker build -t data-engineering .

docker-run:
	docker run --rm -v $(PWD)/data:/app/data data-engineering

# ── Cloud deploy ──────────────────────────────────────────────────────────────

deploy:
	bash scripts/build_lambda.sh
	cd terraform && terraform apply -auto-approve

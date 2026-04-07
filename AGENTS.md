# AGENTS.md

## Cursor Cloud specific instructions

This is a Python data engineering project (e-commerce orders pipeline). There are no external services, databases, or Docker dependencies. Everything runs locally on the filesystem.

### Key commands

| Task | Command |
|---|---|
| Install deps | `pip install -r requirements.txt` |
| Ingest (raw → staging) | `python3 -m pipelines.ingest` |
| Transform (staging → marts) | `python3 -m pipelines.transform` |
| Quality checks | `python3 -m transforms.quality_checks` |
| Run tests | `python3 -m pytest tests/ -v` |

### Notes

- Use `python3` (not `python`) — the environment does not alias `python` to `python3`.
- The pipeline must run in order: ingest before transform, ingest before quality checks.
- Output data lands in `data/staging/` and `data/marts/` (gitignored). These directories are created automatically by the pipeline.
- There is no linter configured in the repo. No `pyproject.toml`, `setup.cfg`, or lint tooling in `requirements.txt`.
- There are no pre-commit hooks or CI/CD pipelines configured.

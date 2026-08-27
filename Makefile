.PHONY: dev build test rehearse docker-up docker-down teardown

dev:
	PYTHONPATH=backend .venv/bin/uvicorn releaseguard.app:app --reload --host 0.0.0.0 --port 8000

build:
	npm run build

test:
	npm run lint
	npm run build
	.venv/bin/pytest -q

rehearse:
	PYTHONPATH=backend .venv/bin/python scripts/rehearse.py

docker-up:
	./scripts/start_demo.sh

docker-down:
	docker compose down

teardown:
	./scripts/teardown_releaseguard.sh

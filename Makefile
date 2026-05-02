# Convenience targets. Everything here is a thin wrapper around docker compose
# or uv — nothing magic. Use the underlying commands directly if you prefer.

.PHONY: install up down logs restart \
        dev-up dev-down dev-logs dev-rebuild dev-shell \
        api worker demo demo-chaos demo-cancel test fmt lint clean

install:
	uv sync --extra dev

# --- production-style containers (baked image) ---------------------------

up:
	docker compose up --build -d
	@echo
	@echo "API:    http://localhost:8000"
	@echo "UI:     http://localhost:8000/ui/index.html"
	@echo "Docs:   http://localhost:8000/docs"

down:
	docker compose down -v

logs:
	docker compose logs -f --tail=80

restart:
	docker compose restart api worker

# --- dev containers (bind-mounted source + auto-reload) ------------------

dev-up:
	docker compose -f docker-compose.dev.yml up --build -d
	@echo
	@echo "DEV API:  http://localhost:8000  (auto-reloads on app/ changes)"
	@echo "DEV UI:   http://localhost:8000/ui/index.html"
	@echo "Logs:     make dev-logs"

dev-down:
	docker compose -f docker-compose.dev.yml down -v

dev-logs:
	docker compose -f docker-compose.dev.yml logs -f --tail=80

dev-rebuild:
	# Run after editing pyproject.toml — picks up new deps inside the venv layer.
	docker compose -f docker-compose.dev.yml build --no-cache

dev-shell:
	docker compose -f docker-compose.dev.yml exec api bash

# --- demos ---------------------------------------------------------------

demo:
	bash demo/happy_path.sh

demo-chaos:
	bash demo/chaos.sh

demo-cancel:
	bash demo/cancel.sh

# --- local (no docker) ---------------------------------------------------

api:
	uv run uvicorn app.api:app --reload --port 8000

worker:
	uv run taskiq worker app.worker:broker app.tasks --workers 1 --reload --reload-dir app

# --- dev tooling ---------------------------------------------------------

test:
	uv run pytest -q

lint:
	uv run ruff check app tests

fmt:
	uv run ruff format app tests
	uv run ruff check --fix app tests

clean:
	docker compose down -v 2>/dev/null || true
	docker compose -f docker-compose.dev.yml down -v 2>/dev/null || true
	rm -rf .venv .pytest_cache .ruff_cache .mypy_cache

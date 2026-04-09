.PHONY: up down restart logs ps \
        init seeds backfill health \
        dbt-run dbt-test dbt-docs \
        api-logs worker-logs \
        clean-volumes

# ─── Docker Compose ───────────────────────────────────────────────────────────

up:
	docker compose up -d

down:
	docker compose down

restart:
	docker compose restart

logs:
	docker compose logs -f --tail=50

ps:
	docker compose ps

# ─── Инициализация ───────────────────────────────────────────────────────────

init: up
	@echo "Waiting for services to be ready..."
	@sleep 15
	docker compose exec airflow-worker-1 \
		python /opt/ducklake-in-practice/scripts/load_seeds.py
	@echo "Seeds loaded."

seeds:
	docker compose exec airflow-worker-1 \
		python /opt/ducklake-in-practice/scripts/load_seeds.py

backfill:
	@if [ -z "$(FROM)" ] || [ -z "$(TO)" ]; then \
		echo "Usage: make backfill FROM=2025-01-01 TO=2025-01-31"; exit 1; \
	fi
	docker compose exec airflow-worker-1 \
		python /opt/ducklake-in-practice/scripts/backfill.py \
		--from $(FROM) --to $(TO)

health:
	docker compose exec airflow-worker-1 \
		python /opt/ducklake-in-practice/scripts/healthcheck.py

# ─── dbt ─────────────────────────────────────────────────────────────────────

dbt-run:
	docker compose exec airflow-worker-1 bash -c \
		"cd /opt/ducklake-in-practice/dbt/ducklake_flights && dbt run"

dbt-test:
	docker compose exec airflow-worker-1 bash -c \
		"cd /opt/ducklake-in-practice/dbt/ducklake_flights && dbt test"

dbt-docs:
	docker compose exec airflow-worker-1 bash -c \
		"cd /opt/ducklake-in-practice/dbt/ducklake_flights && dbt docs generate && dbt docs serve --port 8081"

# ─── Логи ────────────────────────────────────────────────────────────────────

api-logs:
	docker compose logs -f api

worker-logs:
	docker compose logs -f airflow-worker-1 airflow-worker-2

scheduler-logs:
	docker compose logs -f airflow-scheduler

# ─── Очистка ─────────────────────────────────────────────────────────────────

clean-volumes:
	@echo "WARNING: This will delete ALL data (Parquet, PostgreSQL, Redis)."
	@read -p "Are you sure? [y/N] " confirm && [ "$$confirm" = "y" ]
	docker compose down -v
	@echo "Volumes deleted."

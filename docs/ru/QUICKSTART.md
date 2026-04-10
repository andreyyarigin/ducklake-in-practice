# Быстрый старт — ducklake-in-practice

Пошаговое руководство: от клонирования репозитория до работающих дашбордов.

---

## Требования

| Инструмент | Минимальная версия | Зачем |
|---|---|---|
| Docker | 24+ | Все сервисы в контейнерах |
| Docker Compose | v2.20+ | `docker compose` (не `docker-compose`) |
| RAM | 8 GB+ | DuckDB держит данные в памяти при dbt-трансформациях |
| Disk | 5 GB+ | Parquet-файлы + Docker images |

---

## Шаг 1 — Клонирование и конфигурация

```bash
git clone <repo-url>
cd ducklake-in-practice

# Скопировать конфиг окружения
cp .env.example .env
```

Файл `.env` содержит пароли к PostgreSQL, MinIO и Airflow. По умолчанию всё преднастроено для локального запуска — менять не нужно.

---

## Шаг 2 — Запуск сервисов

```bash
docker compose up -d
```

При первом запуске Docker скачает образы (~3–5 GB) и соберёт кастомные Dockerfile. Займёт 5–10 минут.

### Что поднимается

| Сервис | Контейнер | Роль |
|---|---|---|
| MinIO | `dl-minio` | S3-совместимое хранилище Parquet-файлов |
| PostgreSQL | `dl-postgres` | DuckLake каталог + Airflow metadata + Superset appdb |
| Redis | `dl-redis` | Celery broker для Airflow |
| Airflow Webserver | `dl-airflow-webserver` | UI + REST API |
| Airflow Scheduler | `dl-airflow-scheduler` | Планировщик DAG |
| Airflow Worker 1/2 | `dl-airflow-worker-1/2` | Выполнение задач |
| FastAPI | `dl-api` | REST аналитика |
| Superset | `dl-superset` | BI дашборды |

### Проверить статус

```bash
docker compose ps
```

Все сервисы должны быть `healthy`. Airflow и Superset стартуют дольше всего — подождите 2–3 минуты после `docker compose up`.

```bash
# Следить за логами
docker compose logs -f airflow-webserver
```

---

## Шаг 3 — Загрузка справочных данных (seeds)

Seeds загружаются **один раз** при первоначальной настройке. Это аэропорты, авиакомпании, маршруты (OpenFlights), типы ВС и профили маршрутов.

```bash
make seeds
```

Или вручную:

```bash
docker compose exec airflow-worker-1 \
    python /opt/ducklake-in-practice/scripts/load_seeds.py
```

**Что загрузится:**
- `airports` — 177 российских аэропортов
- `airlines` — ~30 активных авиакомпаний РФ
- `routes` — ~600 внутренних маршрутов
- `aircraft_types` — 15 типов воздушных судов
- `route_profiles` — профили маршрутов (load_factor, price_tier, seasonality)

---

## Шаг 4 — Генерация исторических данных (backfill)

Чтобы дашборды показывали данные сразу, нужно наполнить DuckLake за прошедший период.

```bash
# Backfill за последние 90 дней
make backfill FROM=2026-01-01 TO=2026-04-08
```

Или вручную:

```bash
docker compose exec airflow-worker-1 \
    python /opt/ducklake-in-practice/scripts/backfill.py \
    --from 2026-01-01 --to 2026-04-08
```

> **Время выполнения:** ~5–15 минут в зависимости от диапазона дат.

**Что генерируется за каждый день:**
- ~800 рейсов (на основе маршрутов OpenFlights)
- Бронирования по кривой спроса (economy / business / first)
- История цен (dynamic pricing)
- Погода для всех аэропортов (Open-Meteo API)

---

## Шаг 5 — Запуск dbt (трансформации)

dbt трансформирует сырые данные в аналитические marts.

> **Важно:** слои запускаются **раздельными командами** — это не баг, а обязательный workaround для предотвращения OOM при 7.8M строк бронирований.

```bash
# Вариант 1: через Makefile (запускает все слои последовательно)
make dbt-run

# Вариант 2: вручную, слой за слоем
docker compose exec airflow-worker-1 bash -c "
  cd /opt/ducklake-in-practice/dbt/ducklake_flights && \
  dbt deps && \
  dbt run --select staging && \
  dbt run --select int_bookings_daily_agg && \
  dbt run --select intermediate && \
  dbt run --select marts
"
```

**Что создаётся:**

| Слой | Моделей | Описание |
|---|---|---|
| staging | 10 | Очистка, типы, фильтрация |
| intermediate | 4 | Обогащение, денормализация, pre-aggregation |
| marts | 10 | Бизнес-метрики, готовые для Superset и API |

---

## Шаг 6 — Экспорт в Serving Store

Superset и FastAPI читают не из DuckLake напрямую, а из локального DuckDB-файла (`/serving/flights.duckdb`). Экспорт создаёт этот файл атомарно.

```bash
docker compose exec airflow-worker-1 \
    python /opt/ducklake-in-practice/docker/export-serving-store.py
```

> **В боевом режиме:** экспорт запускается автоматически DAG `maintenance` в 03:00 UTC.

---

## Шаг 7 — Открыть интерфейсы

| Сервис | URL | Логин / Пароль |
|---|---|---|
| Airflow | http://localhost:8080 | `admin` / `admin` |
| FastAPI Swagger | http://localhost:8000/docs | — |
| Superset | http://localhost:8088 | `admin` / `admin` |
| MinIO Console | http://localhost:9001 | `minioadmin` / `minioadmin` |

### Superset: открыть дашборд

1. Перейти на http://localhost:8088
2. Войти: `admin` / `admin`
3. Меню → **Dashboards** → **DuckLake Flights Analytics**

### FastAPI: проверить данные

```bash
# Список топ-маршрутов по выручке
curl http://localhost:8000/routes/top?limit=10

# Ежедневные метрики маршрута SVO-LED
curl "http://localhost:8000/routes/SVO-LED/daily?date_from=2026-01-01&date_to=2026-04-01"

# Список снэпшотов DuckLake (time travel)
curl http://localhost:8000/time-travel/snapshots
```

---

## Ежедневный цикл (автоматический)

После первоначальной настройки Airflow запускает все DAG автоматически:

| Время UTC | DAG | Что делает |
|---|---|---|
| 00:30 | `ingest_flights` | Генерирует ~800 рейсов на +7 дней вперёд |
| 00:15, 06:15, 12:15, 18:15 | `ingest_bookings` | Генерирует бронирования (4 батча/день) |
| 01:00 | `ingest_weather` | Загружает погоду из Open-Meteo |
| 02:00 | `dbt_run_daily` | Пересчитывает все dbt-модели |
| 03:00 | `maintenance` | Очистка снэпшотов + экспорт в serving store |
| 03:30 | `dbt_test_daily` | 83 теста качества данных |

---

## Проверка здоровья системы

```bash
make health
```

Или вручную:

```bash
docker compose exec airflow-worker-1 \
    python /opt/ducklake-in-practice/scripts/healthcheck.py
```

Проверяет: DuckLake ATTACH, количество записей в таблицах, свежесть данных (lag > 3h → WARNING).

---

## Полная пересборка (clean start)

```bash
# Удалить все данные и контейнеры
make clean-volumes

# Поднять заново
make init        # = docker compose up -d + seeds
make backfill FROM=2026-01-01 TO=2026-04-08
```

> ⚠️ `make clean-volumes` удаляет **все** Parquet-файлы, PostgreSQL-данные и Redis-состояние. Необратимо.

---

## Устранение неполадок

### Airflow DAG не запускается
```bash
# Проверить логи scheduler
docker compose logs -f airflow-scheduler

# Проверить статус всех сервисов
docker compose ps
```

### dbt падает с OOM
```bash
# Убедиться, что запускаете слои раздельно, а не dbt run без --select
# Проверить, что threads=1 в profiles.yml
cat dbt/ducklake_flights/profiles.yml | grep threads
```

### Superset не показывает данные
```bash
# Проверить, что serving store экспортирован
docker compose exec airflow-worker-1 ls -la /serving/

# Переэкспортировать вручную
docker compose exec airflow-worker-1 \
    python /opt/ducklake-in-practice/docker/export-serving-store.py
```

### DuckLake ATTACH ошибка
```bash
# Проверить, что PostgreSQL и MinIO доступны
docker compose exec airflow-worker-1 \
    python -c "from src.generators.connection import get_ducklake_connection; conn = get_ducklake_connection(); print('OK')"
```

---

## Следующие шаги

- [ARCHITECTURE.md](ARCHITECTURE.md) — подробная архитектура и принятые решения
- [DATA_MODEL.md](DATA_MODEL.md) — все таблицы, поля, партиционирование
- [DBT_LAYERS.md](DBT_LAYERS.md) — dbt-модели, критические фиксы для DuckLake
- [DUCKLAKE_FEATURES.md](DUCKLAKE_FEATURES.md) — ACID-транзакции, time travel, сравнение с Iceberg
- [SCALING.md](SCALING.md) — ограничения и пути роста

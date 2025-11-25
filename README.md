# dwh-smoakland

Local, Docker-Compose–based Airflow starter for the Smoakland automation project.
This repo lets you develop and test Phase 1 ingestion and categorization end-to-end locally, with clean modular separation between:

- Orchestration → Airflow DAGs
- Transform logic → src/transforms/ (pure pandas functions)
- Rule-based categorization → src/rulebook/
- IO adapters → src/io/
- Warehouse adapters → src/warehouse/

Once credentials are available, you can swap local adapters (Drive/DuckDB) for real ones (Google Drive, BigQuery, QuickBooks).

Phase 2 now adds a QuickBooks export path:
- Orchestration → `dags/dag_part2_qbo_export.py` (TaskFlow)
- Pipeline → `src/pipelines/qbo_export.py`
- Domain layer → `src/accounting/` (models, ports, services)
- Integration adapter → `src/integrations/qbo_gateway/` (config, mappers, HTTP client + exporter)
- Exports are built from the categorized warehouse tables and sent to the QBO Gateway Service with idempotent POSTs.

---

## Stack

- Apache Airflow **2.9.3** (Dockerized)
- Python **3.12** (base image)
- pandas
- pydantic (schema validation)
- DuckDB (local warehouse mock)
- Google / HTTP providers for Airflow
- pytest (unit tests)
- docker-compose + Dockerfile (custom build with constraints)

---

## Quickstart

1. **Install Docker Desktop** (Linux/macOS/Windows with WSL2).  
   Make sure it has at least **4GB RAM** allocated.

2. **Set your Airflow UID** (required for permissions):

   - Linux/macOS:
     ```bash
     export AIRFLOW_UID=$(id -u)
     ```
   - Windows (PowerShell):
     ```powershell
     setx AIRFLOW_UID 50000
     ```

3. **Bootstrap Airflow metadata DB and create the admin user**:
   ```bash
   docker compose up airflow-init
   ```

4. **Start the full stack**:
   ```bash
   docker compose up -d --build
   ```
   Local:
   docker compose --profile local -f docker-compose.yml -f docker-compose.local.yml -f docker-compose.gmail.yml up -d

5. **Access Airflow UI**:
   - URL: [http://localhost:8080](http://localhost:8080)  
   - User: `airflow`  
   - Password: `airflow`

### Restoring preconfigured Airflow data

- All exported **Variables**, **Connections**, and a Postgres **metadata dump** live in `airflow_config/`.
- `docker compose up airflow-init` now imports `airflow_config/airflow_vars.json` and `airflow_config/airflow_conns.json` automatically after migrations/user creation.
- The Postgres container mounts `airflow_config/airflow_meta.sql` to `/docker-entrypoint-initdb.d`, so the dump seeds a fresh metadata DB the next time the volume is empty.  
  - To force a restore, drop the existing volume and re-run init:
    ```bash
    docker compose down -v   # removes postgres-db-volume
    docker compose up airflow-init
    docker compose up -d
    ```

---

## Repo Layout

```
.
├── dags/
│   └── dag_part1_ingestion.py     # DAG orchestration (TaskFlow API)
├── src/                           # Core business logic
│   ├── transforms/                # Pure pandas transforms (stateless, testable)
│   │   ├── week_detect.py         # Detect current week boundaries
│   │   ├── normalize.py           # Normalize DataFrames into standard schema
│   │   └── append_logic.py        # Idempotent append logic (avoid duplicates)
│   └── resolve_categorization.py# Universal resolver using all rulebooks
│
│   ├── rulebook/                    # Rule-based classification modules
│   │   ├── payee_vendor.py
│   │   ├── cf_account.py
│   │   ├── dashboard_1.py
│   │   ├── budget_owner.py
│   │   ├── entity_qbo.py
│   │   ├── qbo_account.py
│   │   └── qbo_sub_account.py
│   │
│   ├── io/                        # Input adapters
│   │   ├── storage_base.py        # Abstract interface for storage
│   │   └── storage_local.py       # Local filesystem implementation
│   ├── warehouse/                 # Output adapters
│   │   ├── warehouse_base.py      # Abstract interface for warehouse
│   │   └── warehouse_duckdb.py    # DuckDB implementation (simulates BigQuery)
│   ├── parsers/                   # Source-specific parsers
│   │   ├── base.py                 # Parser interface definition
│   │   ├── utils.py                # Common helpers (clean amounts, txn_id, text collapse)
│   │   ├── keypoint.py             # KeyPoint Credit Union CSV parser
│   │   ├── eastwest.py             # EastWest Bank parser
│   │   ├── dama.py                 # Dama Financial parser
│   │   ├── nbcu.py                 # NBCU parser
│   │   ├── credit_card.py          # Credit Card parser
│   │   └── router.py               # Detect source + select parser
│   └── dq/                        # Data quality checks
│       └── checks.py              # Metrics + validations (duplicates, nulls)
├── tests/                         # Unit tests (pytest)
│   └── test_week_detect.py        # Example test for week detection
├── data_samples/                  # Sample CSVs for local dev
├── plugins/                       # (empty, reserved for Airflow plugins)
├── logs/                          # Airflow logs bind-mounted
├── .env.example                   # Local-only env vars (copy to .env)
├── docker-compose.yml             # Base services (Airflow image, shared config)
├── docker-compose.local.yml       # Overrides for LocalExecutor (dev)
├── docker-compose.celery.yml      # Overrides for CeleryExecutor (scaled/“cloud-like”)
├── Dockerfile                     # Custom Airflow image with constraints
├── requirements.txt               # Extra Python deps (installed at build)
└── README.md

```

---

## Categorization Rulebooks & Overrides

- Payee/vendor resolution is driven by the regex rulebook at `src/rulebook/payee_vendor.py`; rule tags follow `<rulebook>@<version>#<rule_id>`.
- Non-text logic is handled after regex: `categorize_week` calls `postprocess` from `src/rulebook/payee_vendor.py` (wired in `src/transforms/resolve_categorization.py`).
- Current override: if `description` contains the word `CHECK` (case-insensitive, word boundary) and `amount` < 0, then `payee_vendor` is set to `PAYNW` with rule tag `payee_vendor@2025.11.21#post-check-amount` and source `postprocess`.
- Future numeric/cross-field tweaks should follow the same pattern: keep regex-only rules in `_RULES`, and place numeric or multi-field conditions in the rulebook’s `postprocess`.
- Gold output (`gold.categorized_bank_cc`) now derives `week_label` (`Week {week_num}`); `bank_account_cc` comes from a centralized `bank_cc_num` → account mapping (falling back to `bank_account` + `bank_cc_num`); `realme_client_name` is derived from `entity_qbo`; and `description` is normalized by collapsing repeated spaces.

---

## Environment & Config

- Copy `.env.example` → `.env` for **local-only settings**.  
  ⚠️ **Never commit secrets**.
- The `.env` file provides notification-specific values (email list + Slack webhook) consumed by docker-compose overrides like `docker-compose.mailhog.yml`.

- In production: configure **Airflow Connections & Variables** instead of using `.env`.

- PYTHONPATH includes `/opt/airflow/src`, so imports like:

  ```python
  from src.transforms.week_detect import detect_week_bounds
  ```

  work out of the box.

---

## Development Workflow

- Put pure functions in `src/transforms/`.  
- Write adapters in `src/io` and `src/warehouse/`.  
- Orchestrate in `dags/*.py` using Airflow TaskFlow API.  
- Run unit tests locally:
  ```bash
  pytest -v
  ```

---

## Notes

- DuckDB is used locally to **mock BigQuery tables**.  
- Replace `storage_local` / `warehouse_duckdb` with **real Drive/BQ adapters** when credentials are available.  
- Airflow image is built with **constraint files** to avoid dependency conflicts.  
- Weekly ingestion is parameterized using **Airflow Variables** (`WEEK_NUM`, `WEEK_YEAR`):  
  - This allows QA or finance team to force ingestion for a specific ISO week.  
  - If not provided, the DAG falls back to folder naming (`weekNN`) or logical date.  
- Input/Output adapters are configurable via `.env` or Airflow Variables:  
  - `INPUT_FOLDER` points to the local/Drive staging folder with CSVs.  
  - `DUCKDB_PATH` sets the local DuckDB file path (simulates BQ).  
  - `BQ_DATASET` and `BQ_TABLE` are placeholders for future BigQuery integration.  
- This repo is ready to scale:  
  - swap Docker Compose → VPS or K8s  
  - or use managed Airflow (Cloud Composer, Astronomer).

### Phase 2: QBO Export

- DAG `part2_qbo_export` reads the already categorized warehouse data (`gold.categorized_bank_cc`) for the requested week and exports it to QuickBooks through the QBO Gateway Service.
- Window resolution mirrors Phase 1 (Variables `WEEK_YEAR`/`WEEK_NUM`, folder hint, or `logical_date - 7d` fallback).
- Annex A/B mappings are applied end-to-end (domain → payload), including expense class support and the current interim entity_type default for deposits.
- Rows with “unknown” critical fields (vendor/account/bank, etc.) are skipped with a `[qbo-export]` warning to avoid sending bad payloads.
- DAG parameters (Airflow Variables/env):
  - `QBO_CLIENT_ID` (required): UUID/realm used in `/qbo/{client_id}/...`.
  - `QBO_ENVIRONMENT` (`sandbox` or `production`, default `sandbox`).
  - `QBO_AUTO_CREATE` (true/false): only used for sandbox; forced off for production.
  - `QBO_GATEWAY_BASE_URL` (e.g., `http://localhost:8000` or `http://qbo-gateway-api:8000`).
  - `QBO_GATEWAY_API_KEY` (X-API-Key header).
  - Optional: `QBO_GATEWAY_TIMEOUT`, `QBO_GATEWAY_RETRY_ATTEMPTS`, `QBO_GATEWAY_RETRY_BACKOFF`.
- Trigger from Airflow UI: enable `part2_qbo_export`, set the Variables above, and run. The DAG will:
  1) Resolve the target week.
  2) Fetch categorized rows from DuckDB/BigQuery.
  3) Map them into accounting models (Deposits/Expenses) and build QBO payloads.
  4) POST to `/qbo/{client_id}/deposits` and `/expenses` with `X-API-Key`, `Idempotency-Key`, and `environment/auto_create` query params.

#### Alternative Input Source (samples mode)

- Purpose: run Phase 2 without hitting the warehouse by reading sample CSVs from `data_samples/qbo/` with the same schema expected by the exporters.
- Airflow Variable: `QBO_EXPORT_SOURCE` = `warehouse` (default/fallback) or `samples`. Any missing/invalid value falls back to `warehouse`.
- Warehouse mode (default): uses `gold.categorized_bank_cc` via `fetch_categorized_between`.
- Samples mode: loads all CSVs under `data_samples/qbo/`, normalizes bank/account fields, and feeds the same deposit/expense builders (unknown rows are still skipped with warnings).
- Switching modes does not affect Phase 1 ingestion or other `data_samples` folders.

### Phase 2 Architecture Notes

- `src/pipelines/qbo_export.py` orchestrates the export workflow and only depends on the warehouse adapter + accounting services.
- `src/accounting/models.py` defines Pydantic domain models (`Deposit`, `Expense`, etc.) with built-in idempotency fingerprints; `services.py` handles batching and delegates to an exporter interface defined in `ports.py`.
- `src/integrations/qbo_gateway/` provides the concrete exporter:
  - `config.py` reads Airflow Variables/env for base URL, API key, environment, timeouts, retries.
  - `mappers.py` converts domain models into QBO Gateway payloads (Annex A/B style: deposit_to_account/doc_number/lines/private_note and expense vendor/bank_account/lines).
  - `client.py` wraps HTTP calls with headers + retries.
  - `exporter.py` implements the `IAccountingExporter` interface and POSTs to the Gateway with per-entity `Idempotency-Key`.
- Data dependencies: exports read from `gold.categorized_bank_cc`, reusing the categorization outputs produced by Phase 1.
- Local Gateway: point `QBO_GATEWAY_BASE_URL` to your running QBO Gateway (e.g., docker-compose from its repo) and keep `QBO_ENVIRONMENT=sandbox` with `QBO_AUTO_CREATE=true` for smoke tests.

---

## Credentials

- QuickBooks, Google Drive, and BigQuery access will require:
  - Service account credentials (JSON) or API keys.
  - Connections configured in Airflow UI (`Admin → Connections`).  

For local development, you can place placeholder values in `.env` and replace them later.

---

## License

Internal HQ use. Not for redistribution outside Smoakland project.

---

## Email (SMTP)

- For local development, prefer MailHog (no auth):
  - Start with: `docker compose -f docker-compose.yml -f docker-compose.mailhog.yml up -d`
  - Use connection `smtp_default` (auto-created) and view emails at `http://localhost:8025`.

- For Gmail (app password) on port 465/SSL:
  1) Start services with SSL forced and STARTTLS disabled:
     - `docker compose --profile local -f docker-compose.yml -f docker-compose.local.yml -f docker-compose.gmail.yml up -d`
  2) Create the Airflow connection (run inside the webserver container, replace the app password):
     - `airflow connections add --conn-uri "smtps://USER%40DOMAIN:APP_PASSWORD@smtp.gmail.com:465?from_email=USER%40DOMAIN&timeout=30&smtp_ssl=true" smtp_gmail_465`
  3) Test from a shell in the container:
     - `python - <<'PY'
from airflow.utils.email import send_email
send_email(['you@example.com'], 'Gmail smoke test', '<b>Hello</b>', conn_id='smtp_gmail_465')
print('sent')
PY`

Notes:
- Do not commit secrets. Rotate the Gmail app password periodically.
- If your network blocks 465, try port 587 with STARTTLS (`starttls=true`) and remove `docker-compose.gmail.yml` from the stack.

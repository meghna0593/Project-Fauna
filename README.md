# Project-Fauna
An **asynchronous ETL pipeline** for fetching, transforming, and loading animal data into a target system.  
This project demonstrates clean architecture with modular design, retry logic, concurrency, and batch posting.

---

## Features Implemented

- **Async HTTP Client** (`http_client.py`)
  - Built on top of `httpx.AsyncClient`.
  - Retry logic with exponential backoff & jitter.
  - Validation-aware error handling (`422`).

- **API Layer** (`api.py`)
  - Wraps the `animals/v1` endpoints with typed interfaces.
  - Gracefully handles non-JSON responses.

- **Pipeline** (`pipeline.py`)
  - Concurrent fetching of animal details.
  - Transformation:
    - `friends` → split into list of strings.
    - `born_at` → epoch → ISO8601 UTC timestamp.
  - Batch posting (≤100 records per request).

- **CLI Entrypoint** (`cli.py`)
  - Configurable via arguments or environment.
  - Prints runtime config summary.
  - Runs fetch → transform → load.

- **Typed Models** (`models.py`)
  - TypedDicts for raw, detailed, and transformed records.
  - Extensible for schema evolution.

- **Testing**
  - Pytest-based.
  - Fake async clients for retry tests.

---

## Getting Started

### Prerequisites
- Python ≥ 3.10  
- Dependencies: `httpx`, `pytest`
- `make` is usually pre-installed on macOS / Linux
- Run challenge locally:
    ```
        docker load -i lp-programming-challenge-1-1625610904.tar.gz
        docker run --rm -p 3123:3123 -ti lp-programming-challenge-1
    ```

### (Recommended) Steps to run the pipeline using make commands:
1. Create virtual environment and install dependencies
`make setup`
2. Execute pipeline with optional arguments (use `make help` to understand how the commands work)
`make run`
3. Run tests
`make test`
4. Remove venv, cache and build artifacts
`make clean`

### Steps to run the pipeline using virtual environment
1. Create and activate virtual environment
python3 -m venv .venv
source .venv/bin/activate

2. (Optional) Load env vars if you have a .env file
source .env

3. Install dependencies
pip3 install -r requirements.txt

4. Editable install so imports work
pip3 install -e .

4. Run the barebones
animals-etl

5. To run tests
pytest -q

6. Exit virtual environment
deactivate


### Steps to run the script
1. Create and activate virtual environment
python3 -m venv .venv
source .venv/bin/activate

2. (Optional) Load env vars if you have a .env file
source .env

3. Install dependencies
pip3 install -r requirements.txt

4. Run the ETL script
python3 scripts/animals_etl.py

---

## Notes on Concurrency
- The ETL pipeline leverages Python `asyncio` + `httpx` for concurrent fetch / transform / post.
- Running under Uvicorn preserves async concurrency
- This is an **I/O-bound** workload (HTTP calls to the challenge API), so `asyncio` was chosen.
- For **CPU-heavy workloads**, we can consider offloading parts to threads or processes.

---

## Future Enhancements

### Extensibility 
- Add more modules (`birds-etl`, `plants-etl`, etc.) with the same pipeline structure.
- Abstract pipeline stages into reusable building blocks.

### Idempotency
- Ensure re-runs do not duplicate loads by:
    - Storing processed IDs in a local store (`sqlite` or `JSON` file).
    - Implementing "upsert" semantics on POST.

### State Store
- Add optional persistence:
    - `sqlite` backend for checkpoints and processed state.
    - JSON-based fallback for local runs.

### Validation & models
- Use Pydantic models instead of `TypedDict` for:
    - Schema validation.
    - Input/output coercion.
    - Better error messages.

### Concurrency
- Currently **not parallelizing POST** (since batches are fast).
- Add configurable concurrent POST if API latency grows.

### DevOps & Tooling
- Code Quality: integrate linting and formatting.
- Branching Strategy: dev → staging → main.
- PR Template: enforce consistent review process.
- Health Check Endpoint: CLI or /healthz probe.
- Dockerize: containerize for reproducible deployments.
# Project Start 1 — FastAPI ETL Service

A containerised ETL pipeline that ingests customer, order, and order-item data into PostgreSQL and exposes analytical SQL views via a FastAPI REST API.

---

## Project Structure

```
project_start1/
├── .env                        # Environment variables (DB credentials, connection settings)
├── docker-compose.yml          # Defines the api and db services
└── app/
    ├── dockerfile
    ├── requirements.txt
    ├── main.py                 # FastAPI app and endpoint logic
    ├── settings.py             # Singleton settings loaded from environment
    ├── data/
    │   ├── customers.csv
    │   ├── orders.jsonl
    │   └── order_items.csv
    └── factories/
        ├── customers.py        # CREATE / DROP customers table
        ├── orders.py           # CREATE / DROP orders table
        ├── order_items.py      # CREATE / DROP order_items table
        └── views.py            # CREATE / DROP all SQL views
```

---

## Prerequisites

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) installed and running
- (Optional) [Postman](https://www.postman.com/downloads/) for a GUI to call endpoints

---

## Starting the containers

```bash
# From the project root (where docker-compose.yml lives)
docker compose up --build
```

The first run builds the FastAPI image and pulls the PostgreSQL 14 image. On subsequent starts you can omit `--build`.

To stop the containers:

```bash
docker compose down
```

To stop and also delete the database volume (full reset):

```bash
docker compose down -v
```

---

## Calling the API

The API is available at **`http://localhost:8000`** once the containers are running.

You can call endpoints using:

- **Postman** — create a new request, set the method (GET / POST) and paste the URL
- **Web browser** — works for GET endpoints; for POST endpoints use the built-in Swagger UI at `http://localhost:8000/docs`
- **curl** — shown below for reference

---

## Endpoints

Run the endpoints in this order for a clean first-time load:

### 1. Health check
| | |
|---|---|
| **Method** | `GET` |
| **URL** | `http://localhost:8000/health` |
| **Description** | Verifies the API is running and can reach the database. |

```bash
curl http://localhost:8000/health
```

---

### 2. Initialise the database
| | |
|---|---|
| **Method** | `POST` |
| **URL** | `http://localhost:8000/init` |
| **Description** | Drops and recreates the `customers`, `orders`, and `order_items` tables. Run this before `/run` on a fresh start, or whenever you want a clean slate. |

```bash
curl -X POST http://localhost:8000/init
```

---

### 3. Run the ETL pipeline
| | |
|---|---|
| **Method** | `POST` |
| **URL** | `http://localhost:8000/run` |
| **Description** | Reads the three data files, cleans and validates the data, then bulk-loads it into PostgreSQL using `COPY`. Returns row counts and elapsed time. |

**Cleaning applied:**

| Table | Transformations |
|---|---|
| `customers` | Lowercase emails, invalid emails → `NULL`, drop duplicates, `NaN` country codes → `NULL` |
| `orders` | Normalise `order_ts` to UTC, unrecognised statuses → `'unknown'`, orphaned customer IDs dropped |
| `order_items` | `unit_price` forced to absolute value, orphaned order IDs dropped |

```bash
curl -X POST http://localhost:8000/run
```

**Example response:**
```json
{
  "status": "ok",
  "customers_loaded": 5,
  "orders_loaded": 4,
  "order_items_loaded": 4,
  "elapsed_seconds": 0.312
}
```

---

### 4. Create analytical views
| | |
|---|---|
| **Method** | `POST` |
| **URL** | `http://localhost:8000/create_views` |
| **Description** | Creates or replaces all SQL views in the database. |

```bash
curl -X POST http://localhost:8000/create_views
```

---

## SQL Views

Once created, query these views directly in PostgreSQL (e.g. via `psql` or any SQL client).

### Analytical views

| View | Description |
|---|---|
| `vw_daily_metrics` | Per-day order count, total revenue, and average order value |
| `vw_top_customers` | Top 10 customers ranked by lifetime spend |
| `vw_top_skus` | Top 10 SKUs ranked by total revenue, with units sold |

### Data quality views

| View | Description |
|---|---|
| `vw_dq_duplicate_customers` | Customers sharing the same lowercase email |
| `vw_dq_orphaned_orders` | Orders referencing a `customer_id` not in the customers table |
| `vw_dq_invalid_order_items` | Order items with non-positive `quantity` or `unit_price`, with a reason column |
| `vw_dq_invalid_order_status` | Orders whose status is `'unknown'` (mapped from unrecognised values at load time) |

---

## Connecting to PostgreSQL directly

```bash
docker exec -it <db-container-name> psql -U appuser -d appdb
```

You can find the container name with `docker compose ps`.

---

## Environment variables (`.env`)

| Variable | Default | Description |
|---|---|---|
| `POSTGRES_USER` | `appuser` | Database user |
| `POSTGRES_PASSWORD` | `changeme` | Database password — change before production use |
| `POSTGRES_DB` | `appdb` | Database name |
| `POSTGRES_HOST` | `db` | Hostname (service name in docker-compose) |
| `POSTGRES_PORT` | `5432` | PostgreSQL port |

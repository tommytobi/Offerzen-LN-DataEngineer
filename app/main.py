import os
import time
import logging
import psycopg
import pandas as pd
from sqlalchemy import text
from fastapi import FastAPI
from settings import Settings
pd.set_option('display.max_columns', None)

from factories.customers import create_customers, drop_customers
from factories.orders import create_orders, drop_orders
from factories.order_items import create_order_items, drop_order_items
from factories.views import create_views, drop_views
from report_agent import generate_report

# Initialize settings
settings = Settings()
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)

app = FastAPI()


@app.get("/health")
def health():
    logger.info("Health check requested")
    with settings.get_engine().connect() as conn:
        conn.execute(text("select 1"))
        conn.commit()
    return {"status": "ok"}


@app.post("/init")
def run_etl():
    logger.info("ETL requested")
    logger.info("dropping views and tables")
    drop_views(settings)
    drop_order_items(settings)
    drop_orders(settings)
    drop_customers(settings)
    
    logger.info("creating tables")

    # Create tables
    create_customers(settings)
    create_orders(settings)
    create_order_items(settings)
    
    logger.info("ETL finished")
    return {"status": "ok"}

    
@app.post("/run")
def run_query():
    _t_start = time.perf_counter()
    logger.info("ETL started")

    logger.info("ingesting customers.csv")

    df_customers = pd.read_csv(settings.customers_path)
    

    logger.info("cleaning customers data")
    
    df_customers["signup_date"] = pd.to_datetime(
        df_customers["signup_date"], format="mixed", utc=True
    )
    df_customers["email"] = df_customers["email"].str.lower()

    
    _email_re = r"^[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}$"
    df_customers["email"] = df_customers["email"].where(
        df_customers["email"].str.match(_email_re), other=None
    )

    df_customers = df_customers.drop_duplicates(subset="email", keep="first")
    df_customers["country_code"] = df_customers["country_code"].where(
        df_customers["country_code"].notna(), other=None
    )



    logger.info("loading customers into database via COPY")

    with psycopg.connect(
        host=settings.db_host,
        port=settings.db_port,
        dbname=settings.db_name,
        user=settings.db_user,
        password=settings.db_password,
    ) as psycopg_conn:
        with psycopg_conn.cursor() as cur:
            with cur.copy(
                "COPY customers (customer_id, email, full_name, signup_date, country_code, is_active) FROM STDIN"
            ) as copy:
                for row in df_customers.itertuples(index=False):
                    copy.write_row(row)

    logger.info("loaded %d customer rows", len(df_customers))

    ##################################################################################################

    logger.info("ingesting orders.jsonl")
    df_orders = pd.read_json(settings.orders_path, lines=True)

    

    logger.info("parsing and normalizing order_ts to UTC")
    # format="mixed" handles ISO offsets, Z, space/slash separators in one pass;
    # utc=True converts tz-aware values to UTC and localizes naive ones as UTC
    df_orders["order_ts"] = pd.to_datetime(
        df_orders["order_ts"], format="mixed", utc=True
    )

    _valid_statuses = {"placed", "processing", "shipped", "cancelled", "refunded"}
    _invalid_status = ~df_orders["status"].isin(_valid_statuses)
    if _invalid_status.any():
        df_orders.loc[_invalid_status, "status"] = "unknown"

    # removing orders with unknown customer_id
    _valid_customer_ids = set(df_customers["customer_id"].dropna())
    _orphaned = ~df_orders["customer_id"].isin(_valid_customer_ids)
    if _orphaned.any():
        df_orders = df_orders[~_orphaned]

    print(df_orders)

    logger.info("loading orders into database via COPY")
    with psycopg.connect(
        host=settings.db_host,
        port=settings.db_port,
        dbname=settings.db_name,
        user=settings.db_user,
        password=settings.db_password,
    ) as psycopg_conn:
        with psycopg_conn.cursor() as cur:
            with cur.copy(
                "COPY orders (order_id, customer_id, order_ts, status, total_amount, currency) FROM STDIN"
            ) as copy:
                for row in df_orders.itertuples(index=False):
                    copy.write_row(row)

    logger.info("loaded %d order rows", len(df_orders))

    ##################################################################################################

    logger.info("ingesting order_items.csv")
    df_order_items = pd.read_csv(settings.order_items_path)

    df_order_items["unit_price"] = df_order_items["unit_price"].abs()

    _valid_order_ids = set(df_orders["order_id"].dropna())
    _orphaned_items = ~df_order_items["order_id"].isin(_valid_order_ids)
    if _orphaned_items.any():
        df_order_items = df_order_items[~_orphaned_items]

    logger.info("loading order_items into database via COPY")
    with psycopg.connect(
        host=settings.db_host,
        port=settings.db_port,
        dbname=settings.db_name,
        user=settings.db_user,
        password=settings.db_password,
    ) as psycopg_conn:
        with psycopg_conn.cursor() as cur:
            with cur.copy(
                "COPY order_items (order_id, line_no, sku, quantity, unit_price, category) FROM STDIN"
            ) as copy:
                for row in df_order_items.itertuples(index=False):
                    copy.write_row(row)

    logger.info("loaded %d order_item rows", len(df_order_items))

    elapsed = round(time.perf_counter() - _t_start, 3)
    logger.info("ETL finished in %.3fs", elapsed)

    return {
        "status": "ok",
        "customers_loaded": len(df_customers),
        "orders_loaded": len(df_orders),
        "order_items_loaded": len(df_order_items),
        "elapsed_seconds": elapsed,
    }


@app.post("/create_views")
def run_create_views():
    logger.info("creating views")
    create_views(settings)
    logger.info("views created")
    return {"status": "ok"}


@app.post("/report")
def run_report():
    logger.info("report generation requested")
    report_path = generate_report(settings)
    logger.info("report saved to %s", report_path)
    return {"status": "ok", "report_path": str(report_path)}
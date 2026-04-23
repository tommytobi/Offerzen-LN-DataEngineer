from sqlalchemy import text
from settings import Settings


def create_orders(settings: Settings):
    with settings.get_engine().connect() as conn:
        conn.execute(
            text(
                """
        CREATE TABLE IF NOT EXISTS orders (
        order_id BIGINT PRIMARY KEY,
        customer_id INTEGER NOT NULL,
        order_ts TIMESTAMPTZ NOT NULL,
        status TEXT NOT NULL,
        total_amount NUMERIC(12,2) NOT NULL,
        currency CHAR(3),

        CONSTRAINT fk_orders_customer
            FOREIGN KEY (customer_id)
            REFERENCES customers(customer_id)
            ON DELETE CASCADE,

        CONSTRAINT orders_status_chk
            CHECK (status IN ('placed', 'processing', 'shipped', 'cancelled', 'refunded', 'unknown'))
                );
            """)
        )
        conn.commit()

def drop_orders(settings: Settings):
    with settings.get_engine().connect() as conn:
        conn.execute(text("DROP TABLE IF EXISTS orders;"))
        conn.commit()
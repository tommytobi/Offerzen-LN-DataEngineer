from sqlalchemy import text
from settings import Settings


def create_order_items(settings: Settings):
    with settings.get_engine().connect() as conn:
        conn.execute(
            text(
                """
        CREATE TABLE IF NOT EXISTS order_items (
        order_id BIGINT NOT NULL,
        line_no INTEGER NOT NULL,
        sku TEXT NOT NULL,
        quantity INTEGER NOT NULL,
        unit_price NUMERIC(12,2) NOT NULL,
        category TEXT,

        PRIMARY KEY (order_id, line_no),

        CONSTRAINT fk_items_order
            FOREIGN KEY (order_id)
            REFERENCES orders(order_id)
            ON DELETE CASCADE
                );
            """)
        )
        conn.commit()

def drop_order_items(settings: Settings):
    with settings.get_engine().connect() as conn:
        conn.execute(text("DROP TABLE IF EXISTS order_items;"))
        conn.commit()

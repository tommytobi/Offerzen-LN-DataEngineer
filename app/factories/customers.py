from sqlalchemy import text
from settings import Settings


def create_customers(settings: Settings):
    with settings.get_engine().connect() as conn:
        conn.execute(
            text(
                """
        CREATE TABLE IF NOT EXISTS customers (
        customer_id INTEGER PRIMARY KEY,
        email TEXT,
        full_name TEXT,
        signup_date DATE,
        country_code CHAR(2),
        is_active BOOLEAN DEFAULT TRUE,

        CONSTRAINT email_lowercase_chk CHECK (email = LOWER(email))
                );
            """)
        )
        conn.commit()

def drop_customers(settings: Settings):
    with settings.get_engine().connect() as conn:
        conn.execute(text("DROP TABLE IF EXISTS customers;"))
        conn.commit()

from sqlalchemy import text
from settings import Settings


def create_views(settings: Settings):
    with settings.get_engine().connect() as conn:

        # 1. Daily metrics for available data
        conn.execute(text("""
            CREATE OR REPLACE VIEW vw_daily_metrics AS
            SELECT
                order_ts::DATE                          AS date,
                COUNT(*)                                AS orders_count,
                SUM(total_amount)                       AS total_revenue,
                ROUND(AVG(total_amount), 2)             AS average_order_value
            FROM orders
            GROUP BY order_ts::DATE
            ORDER BY date;
        """))

        # 2. Top customers by lifetime spend
        conn.execute(text("""
            CREATE OR REPLACE VIEW vw_top_customers AS
            SELECT
                c.customer_id,
                c.email,
                c.full_name,
                SUM(o.total_amount)                     AS lifetime_spend
            FROM customers c
            JOIN orders o USING (customer_id)
            GROUP BY c.customer_id, c.email, c.full_name
            ORDER BY lifetime_spend DESC
            LIMIT 10;
        """))

        # 3. Top skus by revenue and units sold
        conn.execute(text("""
            CREATE OR REPLACE VIEW vw_top_skus AS
            SELECT
                sku,
                SUM(unit_price * quantity)              AS total_revenue,
                SUM(quantity)                           AS units_sold
            FROM order_items
            GROUP BY sku
            ORDER BY total_revenue DESC
            LIMIT 10;
        """))

        # 4. Customers with duplicate emails
        conn.execute(text("""
            CREATE OR REPLACE VIEW vw_dq_duplicate_customers AS
            SELECT
                LOWER(email)                            AS normalised_email,
                COUNT(*)                                AS occurrences,
                ARRAY_AGG(customer_id ORDER BY customer_id) AS customer_ids
            FROM customers
            WHERE email IS NOT NULL
            GROUP BY LOWER(email)
            HAVING COUNT(*) > 1;
        """))


        # 5. Orders referencing missing customers
        conn.execute(text("""
            CREATE OR REPLACE VIEW vw_dq_orphaned_orders AS
            SELECT o.order_id, o.customer_id, o.order_ts, o.status
            FROM orders o
            LEFT JOIN customers c USING (customer_id)
            WHERE c.customer_id IS NULL;
        """))

        # 6. Order items with non-positive quantity or unit price
        conn.execute(text("""
            CREATE OR REPLACE VIEW vw_dq_invalid_order_items AS
            SELECT
                order_id,
                line_no,
                sku,
                quantity,
                unit_price,
                CASE
                    WHEN quantity   <= 0 AND unit_price <= 0 THEN 'invalid quantity and unit_price'
                    WHEN quantity   <= 0                     THEN 'invalid quantity'
                    WHEN unit_price <= 0                     THEN 'invalid unit_price'
                END AS reason
            FROM order_items
            WHERE quantity <= 0 OR unit_price <= 0;
        """))

        # 7. Orders with a status outside the allowed set
        conn.execute(text("""
            CREATE OR REPLACE VIEW vw_dq_invalid_order_status AS
            SELECT order_id, customer_id, order_ts, status
            FROM orders
            WHERE status = 'unknown';
        """))

        conn.commit()


def drop_views(settings: Settings):
    with settings.get_engine().connect() as conn:
        conn.execute(text("DROP VIEW IF EXISTS vw_dq_invalid_order_status;"))
        conn.execute(text("DROP VIEW IF EXISTS vw_dq_invalid_order_items;"))
        conn.execute(text("DROP VIEW IF EXISTS vw_dq_orphaned_orders;"))
        conn.execute(text("DROP VIEW IF EXISTS vw_dq_duplicate_customers;"))
        conn.execute(text("DROP VIEW IF EXISTS vw_top_skus;"))
        conn.execute(text("DROP VIEW IF EXISTS vw_top_customers;"))
        conn.execute(text("DROP VIEW IF EXISTS vw_daily_metrics;"))
        conn.commit()

# Project Start 1 — FastAPI ETL Service

The design decisions are documented here.

# Task 1

I have created 3 tables in the database that are connected via foreign keys.
The structure looks like this.

customer  <-----  order  <-----  order_item


I have used client side copying to load the data into the database.
This psycopg3 function is one of the fastest ways to load data into a database.


# Task 2

THe following transformation steps are applied to the data

customer data:
1. lowercase emails
2. drop duplicate emails
3. drop invalid emails
4. drop NaN country codes
5. change datetime to UTC

order data:
1. change order_ts to UTC
2. drop invalid order statuses and replace with 'unknown'
3. drop orphaned customer_ids

order items data:
1. make negative unit_prices positive
2. drop orphaned order_ids  


# Task 3

The following views are created
1. vw_daily_metrics
2. vw_top_customers
3. vw_top_skus
4. vw_dq_duplicate_customers
5. vw_dq_orphaned_orders
6. vw_dq_invalid_order_items
7. vw_dq_invalid_order_status   


# Usage of AI

 Ai was used in the following places:

 1. Generation of the report
 2. report agent.py
 3. Used to generate the REGEX for the email validation



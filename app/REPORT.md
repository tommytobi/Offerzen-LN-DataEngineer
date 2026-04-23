# Sales & Data Quality Report

## Executive Summary

Across the six-day window (2024-03-01 to 2024-03-07), the business processed **8 orders** generating **$926.00** in revenue from a top-5 customer base contributing $926.00 in lifetime spend. While headline figures are healthy, ⚠️ **four data quality issues** were identified across order items and order status that are suppressing reported revenue and should be remediated before month-end close.

## Key Metrics

### Daily Order Trends

| Date | Orders | Revenue | AOV |
|---|---|---|---|
| 2024-03-01 | 2 | $370.50 | $185.25 |
| 2024-03-03 | 1 | $60.00 | $60.00 |
| 2024-03-04 | 1 | $0.00 | $0.00 |
| 2024-03-05 | 1 | $10.00 | $10.00 |
| 2024-03-06 | 1 | $220.00 | $220.00 |
| 2024-03-07 | 2 | $265.50 | $132.75 |

No orders were recorded on **2024-03-02**. The 2024-03-04 order posted $0.00 revenue, dragging AOV to zero.

### Top Customers by Lifetime Spend

| Customer | Email | Lifetime Spend |
|---|---|---|
| Jane Doe | jane.doe@example.com | $501.00 |
| John Smith | john.smith@example.com | $340.00 |
| Alex Null | alex.null@example.com | $75.00 |
| No At Sign | ⚠️ NULL | $10.00 |
| Dupe A | dup.email@example.com | $0.00 |

The top 2 customers account for **$841.00 (90.8%)** of lifetime spend.

### Top SKUs by Revenue

| SKU | Revenue | Units |
|---|---|---|
| A-001 | $501.00 | 2 |
| H-654 | $220.00 | 2 |
| B-010 | $120.00 | 2 |
| D-222 | $60.00 | 1 |
| I-111 | $15.00 | 1 |
| G-321 | $10.00 | 1 |
| D-333 | $0.00 | 0 |
| E-777 | $0.00 | 1 |
| H-655 | $0.00 | 1 |

**A-001** alone drives **54%** of revenue.

## Data Quality Findings

### Duplicate Customers
✅ Clean — no duplicates detected.

### Orphaned Orders
✅ Clean — all orders tie to valid customers.

### ⚠️ Invalid Order Items (3 rows)

| Order | Line | SKU | Qty | Price | Issue |
|---|---|---|---|---|---|
| 1004 | 2 | D-333 | 0 | $45.00 | invalid quantity |
| 1005 | 1 | E-777 | 1 | $0.00 | invalid unit_price |
| 1008 | 2 | H-655 | 1 | $0.00 | invalid unit_price |

These three lines explain why SKUs **D-333, E-777, and H-655** show $0.00 revenue.

### ⚠️ Orders with Unknown Status (1 row)

| Order ID | Customer | Timestamp | Status |
|---|---|---|---|
| 1010 | 3 | 2024-03-07 10:00 UTC | unknown |

### ⚠️ Customer Record Quality
Customer ID **6 ("No At Sign")** has a NULL email, blocking transactional communications.

## Notable Observations

1. **Revenue concentration risk**: Jane Doe + John Smith represent 91% of lifetime spend — diversifying the customer base should be a priority.
2. **SKU catalog integrity**: 3 of the top 9 SKUs show zero revenue due to invalid pricing/quantity, not weak demand. Fixing orders **1004, 1005, and 1008** will restore accurate product performance rankings.
3. **Status workflow gap**: Order **1010** with `status='unknown'` suggests a missing enum value or ETL gap — audit the order status pipeline.
4. **Customer 6 email NULL**: Enforce a NOT NULL + regex validation on the `email` column to prevent recurrence.
5. **Monday (2024-03-02) had zero orders** — worth checking whether this reflects genuine demand softness or a data ingestion failure.

**Recommended actions**: (1) correct the 3 invalid line items, (2) resolve order 1010 status, (3) backfill customer 6's email, (4) verify 2024-03-02 ingestion.
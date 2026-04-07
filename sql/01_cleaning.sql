-- ============================================================
--  E-Commerce Funnel & Revenue Drop Analysis
--  STEP 3 — Data Cleaning
--  Creates clean, analysis-ready tables from raw data
--  Run this ONCE after generate_data.py
--  All downstream SQL builds on: clean_users, clean_events,
--  clean_orders
-- ============================================================


-- ────────────────────────────────────────────────────────────
--  SECTION 0 — DROP CLEAN TABLES IF RE-RUNNING
-- ────────────────────────────────────────────────────────────
DROP TABLE IF EXISTS clean_orders  CASCADE;
DROP TABLE IF EXISTS clean_events  CASCADE;
DROP TABLE IF EXISTS clean_users   CASCADE;

DROP TABLE IF EXISTS flagged_skipped_funnel CASCADE;
DROP TABLE IF EXISTS data_quality_report   CASCADE;


-- ============================================================
--  SECTION 1 — CLEAN USERS
--  Issues handled:
--    [1a] NULL device_type   → replaced with 'unknown'
--    [1b] NULL traffic_source → replaced with 'unknown'
--    [1c] Duplicate user_ids  → keep first signup (shouldn't
--         exist due to PK, but guard anyway)
-- ============================================================

CREATE TABLE clean_users AS
WITH deduped AS (
    SELECT
        user_id,
        signup_date,
        country,
        COALESCE(device_type,    'unknown') AS device_type,
        COALESCE(traffic_source, 'unknown') AS traffic_source,
        ROW_NUMBER() OVER (
            PARTITION BY user_id
            ORDER BY signup_date ASC
        ) AS rn
    FROM users
)
SELECT
    user_id,
    signup_date,
    country,
    device_type,
    traffic_source
FROM deduped
WHERE rn = 1;

-- Indexes for join performance
CREATE INDEX idx_cu_user_id        ON clean_users (user_id);
CREATE INDEX idx_cu_device         ON clean_users (device_type);
CREATE INDEX idx_cu_source         ON clean_users (traffic_source);
CREATE INDEX idx_cu_country        ON clean_users (country);

-- ── Verify ──────────────────────────────────────────────────
SELECT
    'clean_users'                                   AS table_name,
    COUNT(*)                                        AS total_rows,
    COUNT(*) FILTER (WHERE device_type    = 'unknown') AS null_device_filled,
    COUNT(*) FILTER (WHERE traffic_source = 'unknown') AS null_source_filled
FROM clean_users;


-- ============================================================
--  SECTION 2 — CLEAN EVENTS
--  Issues handled:
--    [2a] Duplicate events — same user + event_name + timestamp
--         → keep one, discard the rest
--    [2b] Out-of-order timestamps — flag them; keep the event
--         but tag it so analysis can optionally exclude
--    [2c] Delayed events (gap > 7 days between steps) — flag
--    [2d] Events for user_ids not in users table — remove
--    [2e] NULL product_id on product_view / add_to_cart
--         → label as 'UNKNOWN_PRODUCT'
-- ============================================================

CREATE TABLE clean_events AS
WITH

-- [2a] Remove exact duplicates (same user + event_name + timestamp)
deduped AS (
    SELECT
        event_id,
        user_id,
        event_name,
        "timestamp",
        COALESCE(product_id, 'UNKNOWN_PRODUCT') AS product_id,
        ROW_NUMBER() OVER (
            PARTITION BY user_id, event_name, "timestamp"
            ORDER BY event_id
        ) AS rn
    FROM events
),

deduped_clean AS (
    SELECT
        event_id,
        user_id,
        event_name,
        "timestamp",
        product_id
    FROM deduped
    WHERE rn = 1                          -- [2a] keep first, drop duplicates
),

-- [2d] Only keep events for known users
valid_events AS (
    SELECT e.*
    FROM deduped_clean e
    INNER JOIN clean_users u ON e.user_id = u.user_id
),

-- [2b] + [2c] Detect out-of-order & delayed events per user session
ordered AS (
    SELECT
        *,
        LAG("timestamp") OVER (
            PARTITION BY user_id
            ORDER BY "timestamp"
        ) AS prev_timestamp
    FROM valid_events
),

flagged AS (
    SELECT
        event_id,
        user_id,
        event_name,
        "timestamp",
        product_id,
        prev_timestamp,

        -- [2b] out-of-order flag: timestamp < previous event timestamp
        CASE
            WHEN prev_timestamp IS NOT NULL
             AND "timestamp" < prev_timestamp
            THEN TRUE ELSE FALSE
        END AS is_out_of_order,

        -- [2c] delayed event: gap > 7 days since previous event
        CASE
            WHEN prev_timestamp IS NOT NULL
             AND EXTRACT(EPOCH FROM ("timestamp" - prev_timestamp)) > 604800
            THEN TRUE ELSE FALSE
        END AS is_delayed
    FROM ordered
)

SELECT
    event_id,
    user_id,
    event_name,
    "timestamp",
    product_id,
    is_out_of_order,
    is_delayed
FROM flagged;

-- Indexes
CREATE INDEX idx_ce_user_id    ON clean_events (user_id);
CREATE INDEX idx_ce_event_name ON clean_events (event_name);
CREATE INDEX idx_ce_timestamp  ON clean_events ("timestamp");

-- ── Verify ──────────────────────────────────────────────────
SELECT
    'clean_events'                                          AS table_name,
    COUNT(*)                                                AS total_rows,
    COUNT(*) FILTER (WHERE is_out_of_order = TRUE)          AS flagged_out_of_order,
    COUNT(*) FILTER (WHERE is_delayed      = TRUE)          AS flagged_delayed,
    COUNT(*) FILTER (WHERE product_id      = 'UNKNOWN_PRODUCT') AS unknown_product
FROM clean_events;


-- ============================================================
--  SECTION 3 — FLAG SKIPPED FUNNEL STEPS
--  Users who jumped steps (e.g., visit → checkout, skipping
--  product_view and add_to_cart) are flagged separately.
--  We keep them in clean_events but track them here.
-- ============================================================

CREATE TABLE flagged_skipped_funnel AS
WITH user_steps AS (
    SELECT
        user_id,
        ARRAY_AGG(event_name ORDER BY "timestamp") AS steps_taken
    FROM clean_events
    WHERE is_out_of_order = FALSE           -- use only ordered events
    GROUP BY user_id
),
skipped AS (
    SELECT
        user_id,
        steps_taken,
        -- reached checkout without going through add_to_cart
        CASE
            WHEN 'checkout' = ANY(steps_taken)
             AND NOT ('add_to_cart' = ANY(steps_taken))
            THEN TRUE ELSE FALSE
        END AS skipped_add_to_cart,

        -- reached add_to_cart without product_view
        CASE
            WHEN 'add_to_cart' = ANY(steps_taken)
             AND NOT ('product_view' = ANY(steps_taken))
            THEN TRUE ELSE FALSE
        END AS skipped_product_view
    FROM user_steps
)
SELECT *
FROM skipped
WHERE skipped_add_to_cart = TRUE
   OR skipped_product_view = TRUE;

-- ── Verify ──────────────────────────────────────────────────
SELECT
    'flagged_skipped_funnel'                                     AS table_name,
    COUNT(*)                                                     AS users_with_skipped_steps,
    COUNT(*) FILTER (WHERE skipped_add_to_cart  = TRUE)          AS skipped_add_to_cart,
    COUNT(*) FILTER (WHERE skipped_product_view = TRUE)          AS skipped_product_view
FROM flagged_skipped_funnel;


-- ============================================================
--  SECTION 4 — CLEAN ORDERS
--  Issues handled:
--    [4a] Duplicate orders — same user, same amount, within
--         60 seconds → keep earliest
--    [4b] Invalid amounts — zero, negative, or NULL → remove
--    [4c] Anomalous spikes — orders > $5,000 flagged
--    [4d] Orders from unknown users → remove
-- ============================================================

CREATE TABLE clean_orders AS
WITH

-- [4d] only orders for known users
known_user_orders AS (
    SELECT o.*
    FROM orders o
    INNER JOIN clean_users u ON o.user_id = u.user_id
),

-- [4b] remove invalid amounts
valid_amount AS (
    SELECT *
    FROM known_user_orders
    WHERE order_amount IS NOT NULL
      AND order_amount > 0
),

-- [4a] deduplicate: same user + same amount within 60 seconds
deduped AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY
                user_id,
                order_amount,
                DATE_TRUNC('minute', order_timestamp)  -- 60-sec bucket
            ORDER BY order_timestamp ASC
        ) AS rn
    FROM valid_amount
),

deduped_clean AS (
    SELECT
        order_id,
        user_id,
        order_amount,
        payment_status,
        order_timestamp
    FROM deduped
    WHERE rn = 1
)

-- [4c] flag anomalous spikes (keep in table, flag for analysis)
SELECT
    order_id,
    user_id,
    order_amount,
    payment_status,
    order_timestamp,
    CASE
        WHEN order_amount > 5000 THEN TRUE
        ELSE FALSE
    END AS is_anomalous
FROM deduped_clean;

-- Indexes
CREATE INDEX idx_co_user_id   ON clean_orders (user_id);
CREATE INDEX idx_co_status    ON clean_orders (payment_status);
CREATE INDEX idx_co_timestamp ON clean_orders (order_timestamp);

-- ── Verify ──────────────────────────────────────────────────
SELECT
    'clean_orders'                                              AS table_name,
    COUNT(*)                                                    AS total_rows,
    COUNT(*) FILTER (WHERE payment_status = 'failed')           AS failed_payments,
    COUNT(*) FILTER (WHERE payment_status = 'success')          AS successful_payments,
    COUNT(*) FILTER (WHERE is_anomalous   = TRUE)               AS anomalous_orders,
    ROUND(
        COUNT(*) FILTER (WHERE payment_status = 'failed')
        * 100.0 / COUNT(*), 2
    )                                                           AS failed_payment_pct,
    ROUND(SUM(order_amount) FILTER (WHERE payment_status = 'success'), 2) AS total_revenue,
    ROUND(AVG(order_amount) FILTER (WHERE payment_status = 'success'), 2) AS avg_order_value
FROM clean_orders;


-- ============================================================
--  SECTION 5 — DATA QUALITY REPORT
--  Summary of all issues found and handled
--  Useful for the dashboard's "Data Quality" panel
-- ============================================================

DROP TABLE IF EXISTS data_quality_report;

CREATE TABLE data_quality_report AS
SELECT 'duplicate_events_removed' AS issue,
       (SELECT COUNT(*) FROM events) - (SELECT COUNT(*) FROM clean_events) AS records_affected,
       'Removed' AS action_taken
UNION ALL
SELECT 'out_of_order_events_flagged',
       COUNT(*) FILTER (WHERE is_out_of_order = TRUE),
       'Flagged'
FROM clean_events
UNION ALL
SELECT 'delayed_events_flagged',
       COUNT(*) FILTER (WHERE is_delayed = TRUE),
       'Flagged'
FROM clean_events
UNION ALL
SELECT 'null_device_filled',
       COUNT(*) FILTER (WHERE device_type = 'unknown'),
       'Filled as unknown'
FROM clean_users
UNION ALL
SELECT 'null_traffic_source_filled',
       COUNT(*) FILTER (WHERE traffic_source = 'unknown'),
       'Filled as unknown'
FROM clean_users
UNION ALL
SELECT 'duplicate_orders_removed',
       (SELECT COUNT(*) FROM orders) - (SELECT COUNT(*) FROM clean_orders),
       'Removed'
UNION ALL
SELECT 'failed_payments_flagged',
       COUNT(*) FILTER (WHERE payment_status = 'failed'),
       'Flagged'
FROM clean_orders
UNION ALL
SELECT 'anomalous_order_amounts_flagged',
       COUNT(*) FILTER (WHERE is_anomalous = TRUE),
       'Flagged'
FROM clean_orders
UNION ALL
SELECT 'users_with_skipped_funnel_steps',
       COUNT(*),
       'Flagged'
FROM flagged_skipped_funnel;



-- ── Final Quality Report ─────────────────────────────────────
SELECT
    issue,
    records_affected,
    action_taken
FROM data_quality_report
ORDER BY records_affected DESC;
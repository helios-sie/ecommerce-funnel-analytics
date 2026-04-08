-- ============================================================
--  E-Commerce Funnel & Revenue Drop Analysis
--  STEP 4 — Funnel Analysis
--  Answers: Where exactly are users dropping off?
--  Builds on: clean_users, clean_events, clean_orders
--  Output tables:
--    → funnel_stage_metrics
--    → funnel_dropoff_revenue
--    → funnel_summary
-- ============================================================


-- ────────────────────────────────────────────────────────────
--  DROP EXISTING TABLES IF RE-RUNNING
-- ────────────────────────────────────────────────────────────
DROP TABLE IF EXISTS funnel_summary          CASCADE;
DROP TABLE IF EXISTS funnel_dropoff_revenue  CASCADE;
DROP TABLE IF EXISTS funnel_stage_metrics    CASCADE;


-- ============================================================
--  SECTION 1 — USERS AT EACH FUNNEL STAGE
--  Count distinct users who reached each stage
--  Uses clean_events only (duplicates already removed)
-- ============================================================

CREATE TABLE funnel_stage_metrics AS
WITH

-- Step 1: Get one row per user per stage (deduplicated)
user_stage AS (
    SELECT DISTINCT
        user_id,
        event_name AS stage
    FROM clean_events
    WHERE event_name IN ('visit','product_view','add_to_cart','checkout','payment')
),

-- Step 2: Count users at each stage
stage_counts AS (
    SELECT
        stage,
        COUNT(DISTINCT user_id) AS users_at_stage
    FROM user_stage
    GROUP BY stage
),

-- Step 3: Attach a sort order so funnel renders top → bottom
stage_ordered AS (
    SELECT
        stage,
        users_at_stage,
        CASE stage
            WHEN 'visit'        THEN 1
            WHEN 'product_view' THEN 2
            WHEN 'add_to_cart'  THEN 3
            WHEN 'checkout'     THEN 4
            WHEN 'payment'      THEN 5
        END AS stage_order
    FROM stage_counts
),

-- Step 4: Add previous stage user count for drop-off calculation
with_prev AS (
    SELECT
        stage,
        stage_order,
        users_at_stage,
        LAG(users_at_stage) OVER (ORDER BY stage_order) AS prev_stage_users
    FROM stage_ordered
),

-- Step 5: Calculate conversion and drop-off metrics
metrics AS (
    SELECT
        stage_order,
        stage,
        users_at_stage,
        prev_stage_users,

        -- Users lost between this stage and previous
        COALESCE(prev_stage_users - users_at_stage, 0)       AS users_dropped,

        -- Stage-to-stage conversion rate (e.g. cart → checkout)
        CASE
            WHEN prev_stage_users IS NULL THEN 100.00
            ELSE ROUND(users_at_stage * 100.0 / prev_stage_users, 2)
        END AS stage_conversion_pct,

        -- Drop-off rate from previous stage
        CASE
            WHEN prev_stage_users IS NULL THEN 0.00
            ELSE ROUND((prev_stage_users - users_at_stage) * 100.0 / prev_stage_users, 2)
        END AS stage_dropoff_pct,

        -- Overall conversion rate (relative to visit/top of funnel)
        ROUND(
            users_at_stage * 100.0 /
            FIRST_VALUE(users_at_stage) OVER (ORDER BY stage_order),
        2) AS overall_conversion_pct
    FROM with_prev
)

SELECT
    stage_order,
    stage,
    users_at_stage,
    prev_stage_users,
    users_dropped,
    stage_conversion_pct,
    stage_dropoff_pct,
    overall_conversion_pct
FROM metrics
ORDER BY stage_order;

-- ── Quick check ──────────────────────────────────────────────
SELECT
    stage_order,
    stage,
    users_at_stage,
    users_dropped,
    stage_conversion_pct   AS "stage→next %",
    stage_dropoff_pct      AS "drop-off %",
    overall_conversion_pct AS "overall %"
FROM funnel_stage_metrics
ORDER BY stage_order;


-- ============================================================
--  SECTION 2 — REVENUE LOST AT EACH DROP-OFF POINT
--  Attaches a dollar value to every stage where users exit
--
--  Logic:
--    avg_order_value (from successful orders)
--    × users_dropped_at_stage
--    × overall_payment_conversion_rate
--    = estimated revenue lost at that stage
-- ============================================================

CREATE TABLE funnel_dropoff_revenue AS
WITH

-- Average order value from successful orders (excluding anomalies)
aov AS (
    SELECT
        ROUND(AVG(order_amount), 2) AS avg_order_value
    FROM clean_orders
    WHERE payment_status = 'success'
      AND is_anomalous   = FALSE
),

-- Payment conversion rate = users who paid / users who visited
payment_conv AS (
    SELECT
        ROUND(
            MAX(CASE WHEN stage = 'payment' THEN users_at_stage END) * 1.0 /
            MAX(CASE WHEN stage = 'visit'   THEN users_at_stage END),
        4) AS payment_rate
    FROM funnel_stage_metrics
),

-- Join drop-off data with revenue estimates
revenue_loss AS (
    SELECT
        f.stage_order,
        f.stage,
        f.users_dropped,
        a.avg_order_value,
        p.payment_rate,

        -- Estimated revenue lost = dropped users × AOV × chance they'd have converted
        ROUND(f.users_dropped * a.avg_order_value * p.payment_rate, 2) AS estimated_revenue_lost
    FROM funnel_stage_metrics f
    CROSS JOIN aov a
    CROSS JOIN payment_conv p
    WHERE f.users_dropped > 0
)

SELECT
    stage_order,
    stage,
    users_dropped,
    avg_order_value,
    payment_rate,
    estimated_revenue_lost
FROM revenue_loss
ORDER BY stage_order;

-- ── Quick check ──────────────────────────────────────────────
SELECT
    stage,
    users_dropped,
    avg_order_value,
    ROUND(payment_rate * 100, 2)   AS payment_conv_pct,
    estimated_revenue_lost
FROM funnel_dropoff_revenue
ORDER BY stage_order;


-- ============================================================
--  SECTION 3 — FUNNEL SUMMARY
--  Single-row executive summary of the entire funnel
--  Used by the dashboard's KPI cards and API /funnel endpoint
-- ============================================================

CREATE TABLE funnel_summary AS
WITH

top_stage AS (
    SELECT users_at_stage AS total_visitors
    FROM funnel_stage_metrics
    WHERE stage = 'visit'
),

bottom_stage AS (
    SELECT users_at_stage AS total_converters
    FROM funnel_stage_metrics
    WHERE stage = 'payment'
),

revenue_totals AS (
    SELECT
        ROUND(SUM(order_amount), 2)  AS total_revenue,
        ROUND(AVG(order_amount), 2)  AS avg_order_value,
        COUNT(*)                     AS total_orders,
        COUNT(*) FILTER (WHERE payment_status = 'failed')  AS failed_orders,
        ROUND(SUM(order_amount) FILTER (WHERE payment_status = 'failed'), 2) AS revenue_from_failed
    FROM clean_orders
),

biggest_dropoff AS (
    SELECT stage, users_dropped
    FROM funnel_stage_metrics
    ORDER BY users_dropped DESC
    LIMIT 1
),

total_lost AS (
    SELECT ROUND(SUM(estimated_revenue_lost), 2) AS total_revenue_lost
    FROM funnel_dropoff_revenue
)

SELECT
    t.total_visitors,
    b.total_converters,
    ROUND(b.total_converters * 100.0 / t.total_visitors, 2) AS end_to_end_conversion_pct,
    r.total_revenue,
    r.avg_order_value,
    r.total_orders,
    r.failed_orders,
    ROUND(r.failed_orders * 100.0 / r.total_orders, 2)      AS failed_payment_pct,
    r.revenue_from_failed                                    AS revenue_lost_to_failures,
    d.stage                                                  AS biggest_dropoff_stage,
    d.users_dropped                                          AS biggest_dropoff_users,
    l.total_revenue_lost                                     AS total_estimated_revenue_lost
FROM top_stage      t
CROSS JOIN bottom_stage   b
CROSS JOIN revenue_totals r
CROSS JOIN biggest_dropoff d
CROSS JOIN total_lost      l;

-- ── Executive Summary Output ─────────────────────────────────
SELECT
    total_visitors,
    total_converters,
    end_to_end_conversion_pct       AS "end-to-end conv %",
    total_revenue                   AS "actual revenue ($)",
    avg_order_value                 AS "AOV ($)",
    total_orders,
    failed_orders,
    failed_payment_pct              AS "failed payment %",
    revenue_lost_to_failures        AS "lost to failures ($)",
    biggest_dropoff_stage           AS "worst stage",
    biggest_dropoff_users           AS "users lost there",
    total_estimated_revenue_lost    AS "total est. lost ($)"
FROM funnel_summary;

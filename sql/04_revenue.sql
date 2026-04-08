-- =============================================================================
-- revenue.sql
-- E-Commerce Funnel & Revenue Drop Analysis — Step 6
-- Computes actual revenue, AOV, and failed payment losses by segment
-- Depends on: clean_users, clean_orders, segment_device_funnel,
--             segment_source_funnel, segment_country_funnel
-- Run each section separately in DBeaver: highlight block → Ctrl+Enter
-- =============================================================================


-- =============================================================================
-- SECTION 1: revenue_overall
-- Single-row global revenue summary — the top-level KPI card
-- =============================================================================

DROP TABLE IF EXISTS revenue_overall;

CREATE TABLE revenue_overall AS
SELECT
    COUNT(*)                                                                    AS total_orders,
    COUNT(CASE WHEN payment_status = 'success' THEN 1 END)                     AS successful_orders,
    COUNT(CASE WHEN payment_status = 'failed'  THEN 1 END)                     AS failed_orders,

    ROUND(SUM(CASE WHEN payment_status = 'success' THEN order_amount ELSE 0 END), 2)
                                                                                AS total_revenue,
    ROUND(SUM(CASE WHEN payment_status = 'failed'  THEN order_amount ELSE 0 END), 2)
                                                                                AS failed_payment_revenue_lost,

    ROUND(AVG(CASE WHEN payment_status = 'success' THEN order_amount END), 2)  AS aov,
    ROUND(AVG(CASE WHEN payment_status = 'failed'  THEN order_amount END), 2)  AS avg_failed_order_value,

    ROUND(
        COUNT(CASE WHEN payment_status = 'failed' THEN 1 END)::NUMERIC
        / NULLIF(COUNT(*), 0) * 100, 2
    )                                                                           AS failed_payment_rate_pct,

    ROUND(
        SUM(CASE WHEN payment_status = 'failed' THEN order_amount ELSE 0 END)
        / NULLIF(SUM(order_amount), 0) * 100, 2
    )                                                                           AS failed_revenue_rate_pct,

    -- Anomalous orders (amount > $5,000) — flagged in cleaning
    COUNT(CASE WHEN is_anomalous = TRUE THEN 1 END)                            AS anomalous_orders,
    ROUND(SUM(CASE WHEN is_anomalous = TRUE THEN order_amount ELSE 0 END), 2)  AS anomalous_revenue

FROM clean_orders;

-- Verify
SELECT * FROM revenue_overall;


-- =============================================================================
-- SECTION 2: revenue_by_device
-- Revenue, AOV, and failed payment loss broken down by device_type
-- Joins clean_orders → clean_users for device dimension
-- =============================================================================

DROP TABLE IF EXISTS revenue_by_device;

CREATE TABLE revenue_by_device AS
SELECT
    cu.device_type,

    COUNT(co.order_id)                                                          AS total_orders,
    COUNT(CASE WHEN co.payment_status = 'success' THEN 1 END)                  AS successful_orders,
    COUNT(CASE WHEN co.payment_status = 'failed'  THEN 1 END)                  AS failed_orders,

    ROUND(SUM(CASE WHEN co.payment_status = 'success' THEN co.order_amount ELSE 0 END), 2)
                                                                                AS total_revenue,
    ROUND(SUM(CASE WHEN co.payment_status = 'failed'  THEN co.order_amount ELSE 0 END), 2)
                                                                                AS failed_payment_revenue_lost,

    ROUND(AVG(CASE WHEN co.payment_status = 'success' THEN co.order_amount END), 2)
                                                                                AS aov,

    ROUND(
        COUNT(CASE WHEN co.payment_status = 'failed' THEN 1 END)::NUMERIC
        / NULLIF(COUNT(co.order_id), 0) * 100, 2
    )                                                                           AS failed_payment_rate_pct,

    -- Revenue share of total
    ROUND(
        SUM(CASE WHEN co.payment_status = 'success' THEN co.order_amount ELSE 0 END)
        / NULLIF(SUM(SUM(CASE WHEN co.payment_status = 'success' THEN co.order_amount ELSE 0 END)) OVER (), 0)
        * 100, 2
    )                                                                           AS revenue_share_pct,

    -- Pull in funnel conversion rate from segment_device_funnel for context
    sdf.end_to_end_conv_pct,
    sdf.users_visit,
    sdf.traffic_share_pct

FROM clean_orders co
JOIN clean_users cu ON co.user_id = cu.user_id
LEFT JOIN segment_device_funnel sdf ON cu.device_type = sdf.device_type
GROUP BY cu.device_type, sdf.end_to_end_conv_pct, sdf.users_visit, sdf.traffic_share_pct
ORDER BY total_revenue DESC;

CREATE INDEX idx_rev_device ON revenue_by_device(device_type);

-- Verify
SELECT
    device_type,
    total_orders,
    total_revenue,
    failed_payment_revenue_lost,
    aov,
    failed_payment_rate_pct,
    revenue_share_pct,
    end_to_end_conv_pct
FROM revenue_by_device
ORDER BY total_revenue DESC;


-- =============================================================================
-- SECTION 3: revenue_by_source
-- Revenue, AOV, and failed payment loss broken down by traffic_source
-- =============================================================================

DROP TABLE IF EXISTS revenue_by_source;

CREATE TABLE revenue_by_source AS
SELECT
    cu.traffic_source,

    COUNT(co.order_id)                                                          AS total_orders,
    COUNT(CASE WHEN co.payment_status = 'success' THEN 1 END)                  AS successful_orders,
    COUNT(CASE WHEN co.payment_status = 'failed'  THEN 1 END)                  AS failed_orders,

    ROUND(SUM(CASE WHEN co.payment_status = 'success' THEN co.order_amount ELSE 0 END), 2)
                                                                                AS total_revenue,
    ROUND(SUM(CASE WHEN co.payment_status = 'failed'  THEN co.order_amount ELSE 0 END), 2)
                                                                                AS failed_payment_revenue_lost,

    ROUND(AVG(CASE WHEN co.payment_status = 'success' THEN co.order_amount END), 2)
                                                                                AS aov,

    ROUND(
        COUNT(CASE WHEN co.payment_status = 'failed' THEN 1 END)::NUMERIC
        / NULLIF(COUNT(co.order_id), 0) * 100, 2
    )                                                                           AS failed_payment_rate_pct,

    -- Revenue per visitor = total_revenue / users_visit (efficiency metric)
    ROUND(
        SUM(CASE WHEN co.payment_status = 'success' THEN co.order_amount ELSE 0 END)
        / NULLIF(ssf.users_visit, 0), 2
    )                                                                           AS revenue_per_visitor,

    -- Revenue share of total
    ROUND(
        SUM(CASE WHEN co.payment_status = 'success' THEN co.order_amount ELSE 0 END)
        / NULLIF(SUM(SUM(CASE WHEN co.payment_status = 'success' THEN co.order_amount ELSE 0 END)) OVER (), 0)
        * 100, 2
    )                                                                           AS revenue_share_pct,

    ssf.end_to_end_conv_pct,
    ssf.source_quality_score,
    ssf.users_visit,
    ssf.traffic_share_pct

FROM clean_orders co
JOIN clean_users cu ON co.user_id = cu.user_id
LEFT JOIN segment_source_funnel ssf ON cu.traffic_source = ssf.traffic_source
GROUP BY cu.traffic_source, ssf.end_to_end_conv_pct, ssf.source_quality_score,
         ssf.users_visit, ssf.traffic_share_pct
ORDER BY total_revenue DESC;

CREATE INDEX idx_rev_source ON revenue_by_source(traffic_source);

-- Verify
SELECT
    traffic_source,
    total_orders,
    total_revenue,
    failed_payment_revenue_lost,
    aov,
    revenue_per_visitor,
    failed_payment_rate_pct,
    revenue_share_pct,
    source_quality_score
FROM revenue_by_source
ORDER BY revenue_per_visitor DESC;


-- =============================================================================
-- SECTION 4: revenue_by_country
-- Revenue, AOV, and failed payment loss broken down by country
-- =============================================================================

DROP TABLE IF EXISTS revenue_by_country;

CREATE TABLE revenue_by_country AS
SELECT
    cu.country,

    COUNT(co.order_id)                                                          AS total_orders,
    COUNT(CASE WHEN co.payment_status = 'success' THEN 1 END)                  AS successful_orders,
    COUNT(CASE WHEN co.payment_status = 'failed'  THEN 1 END)                  AS failed_orders,

    ROUND(SUM(CASE WHEN co.payment_status = 'success' THEN co.order_amount ELSE 0 END), 2)
                                                                                AS total_revenue,
    ROUND(SUM(CASE WHEN co.payment_status = 'failed'  THEN co.order_amount ELSE 0 END), 2)
                                                                                AS failed_payment_revenue_lost,

    ROUND(AVG(CASE WHEN co.payment_status = 'success' THEN co.order_amount END), 2)
                                                                                AS aov,

    ROUND(
        COUNT(CASE WHEN co.payment_status = 'failed' THEN 1 END)::NUMERIC
        / NULLIF(COUNT(co.order_id), 0) * 100, 2
    )                                                                           AS failed_payment_rate_pct,

    -- Revenue per visitor
    ROUND(
        SUM(CASE WHEN co.payment_status = 'success' THEN co.order_amount ELSE 0 END)
        / NULLIF(scf.users_visit, 0), 2
    )                                                                           AS revenue_per_visitor,

    -- Revenue share of total
    ROUND(
        SUM(CASE WHEN co.payment_status = 'success' THEN co.order_amount ELSE 0 END)
        / NULLIF(SUM(SUM(CASE WHEN co.payment_status = 'success' THEN co.order_amount ELSE 0 END)) OVER (), 0)
        * 100, 2
    )                                                                           AS revenue_share_pct,

    scf.end_to_end_conv_pct,
    scf.users_visit,
    scf.traffic_share_pct

FROM clean_orders co
JOIN clean_users cu ON co.user_id = cu.user_id
LEFT JOIN segment_country_funnel scf ON cu.country = scf.country
GROUP BY cu.country, scf.end_to_end_conv_pct, scf.users_visit, scf.traffic_share_pct
ORDER BY total_revenue DESC;

CREATE INDEX idx_rev_country ON revenue_by_country(country);

-- Verify
SELECT
    country,
    total_orders,
    total_revenue,
    failed_payment_revenue_lost,
    aov,
    revenue_per_visitor,
    failed_payment_rate_pct,
    revenue_share_pct,
    end_to_end_conv_pct
FROM revenue_by_country
ORDER BY total_revenue DESC;


-- =============================================================================
-- SECTION 5: revenue_monthly_trend
-- Monthly revenue, order volume, AOV, and failed payment trend for 2024
-- Used by the dashboard time-series chart
-- =============================================================================

-- =============================================================================
-- revenue.sql — SECTION 5 FIX: revenue_monthly_trend
-- Replace Section 5 with this. The fix wraps the aggregation in a subquery
-- so that the LAG() window function can reference monthly_revenue by name.
-- =============================================================================

DROP TABLE IF EXISTS revenue_monthly_trend;

CREATE TABLE revenue_monthly_trend AS
SELECT
    month,
    month_label,
    total_orders,
    successful_orders,
    failed_orders,
    monthly_revenue,
    monthly_failed_revenue,
    monthly_aov,
    failed_payment_rate_pct,

    ROUND(
        monthly_revenue - LAG(monthly_revenue) OVER (ORDER BY month),
        2
    ) AS mom_revenue_change,

    ROUND(
        (monthly_revenue - LAG(monthly_revenue) OVER (ORDER BY month))
        / NULLIF(LAG(monthly_revenue) OVER (ORDER BY month), 0) * 100,
        2
    ) AS mom_revenue_change_pct

FROM (
    SELECT
        DATE_TRUNC('month', order_timestamp)::DATE                                  AS month,
        TO_CHAR(order_timestamp, 'Mon YYYY')                                        AS month_label,

        COUNT(order_id)                                                             AS total_orders,
        COUNT(CASE WHEN payment_status = 'success' THEN 1 END)                     AS successful_orders,
        COUNT(CASE WHEN payment_status = 'failed'  THEN 1 END)                     AS failed_orders,

        ROUND(SUM(CASE WHEN payment_status = 'success' THEN order_amount ELSE 0 END), 2)
                                                                                    AS monthly_revenue,
        ROUND(SUM(CASE WHEN payment_status = 'failed'  THEN order_amount ELSE 0 END), 2)
                                                                                    AS monthly_failed_revenue,

        ROUND(AVG(CASE WHEN payment_status = 'success' THEN order_amount END), 2)  AS monthly_aov,

        ROUND(
            COUNT(CASE WHEN payment_status = 'failed' THEN 1 END)::NUMERIC
            / NULLIF(COUNT(order_id), 0) * 100, 2
        )                                                                           AS failed_payment_rate_pct

    FROM clean_orders
    GROUP BY DATE_TRUNC('month', order_timestamp), TO_CHAR(order_timestamp, 'Mon YYYY')
) monthly_agg
ORDER BY month;

CREATE INDEX idx_rev_monthly ON revenue_monthly_trend(month);

-- Verify
SELECT
    month_label,
    total_orders,
    monthly_revenue,
    monthly_aov,
    failed_payment_rate_pct,
    mom_revenue_change,
    mom_revenue_change_pct
FROM revenue_monthly_trend
ORDER BY month;



-- =============================================================================
-- SECTION 6: revenue_summary
-- Single executive row — all key revenue KPIs in one place
-- Combines revenue_overall with segment bests/worsts
-- =============================================================================

DROP TABLE IF EXISTS revenue_summary;

CREATE TABLE revenue_summary AS
SELECT
    -- Global KPIs (from revenue_overall)
    ro.total_orders,
    ro.successful_orders,
    ro.failed_orders,
    ro.total_revenue,
    ro.failed_payment_revenue_lost,
    ro.aov,
    ro.failed_payment_rate_pct,
    ro.failed_revenue_rate_pct,

    -- Best and worst device by revenue
    (SELECT device_type FROM revenue_by_device ORDER BY total_revenue DESC LIMIT 1)
                                                                                AS top_device_by_revenue,
    (SELECT device_type FROM revenue_by_device ORDER BY aov DESC LIMIT 1)
                                                                                AS top_device_by_aov,
    (SELECT device_type FROM revenue_by_device ORDER BY failed_payment_rate_pct DESC LIMIT 1)
                                                                                AS worst_device_failed_payments,

    -- Best and worst source by revenue
    (SELECT traffic_source FROM revenue_by_source ORDER BY total_revenue DESC LIMIT 1)
                                                                                AS top_source_by_revenue,
    (SELECT traffic_source FROM revenue_by_source ORDER BY revenue_per_visitor DESC LIMIT 1)
                                                                                AS most_efficient_source,
    (SELECT traffic_source FROM revenue_by_source ORDER BY failed_payment_rate_pct DESC LIMIT 1)
                                                                                AS worst_source_failed_payments,

    -- Best and worst country by revenue
    (SELECT country FROM revenue_by_country ORDER BY total_revenue DESC LIMIT 1)
                                                                                AS top_country_by_revenue,
    (SELECT country FROM revenue_by_country ORDER BY revenue_per_visitor DESC LIMIT 1)
                                                                                AS most_efficient_country,
    (SELECT country FROM revenue_by_country ORDER BY failed_payment_revenue_lost DESC LIMIT 1)
                                                                                AS country_biggest_payment_loss,

    -- Monthly trend context
    (SELECT month_label FROM revenue_monthly_trend ORDER BY monthly_revenue DESC LIMIT 1)
                                                                                AS best_revenue_month,
    (SELECT month_label FROM revenue_monthly_trend ORDER BY monthly_revenue ASC  LIMIT 1)
                                                                                AS worst_revenue_month,
    (SELECT ROUND(AVG(failed_payment_rate_pct), 2) FROM revenue_monthly_trend)  AS avg_monthly_failed_rate_pct

FROM revenue_overall ro;

-- Verify
	SELECT * FROM revenue_summary;


-- =============================================================================
-- SECTION 7: DIAGNOSTIC QUERIES
-- Standalone business-answer queries — run after all tables are built
-- =============================================================================

-- Q1: Full revenue breakdown by device with efficiency comparison
SELECT
    'REVENUE BY DEVICE' AS report,
    device_type,
    total_orders,
    total_revenue,
    aov,
    failed_payment_revenue_lost,
    failed_payment_rate_pct,
    revenue_share_pct,
    end_to_end_conv_pct
FROM revenue_by_device
ORDER BY total_revenue DESC;

-- Q2: Source efficiency — revenue per visitor ranks sources by true ROI
SELECT
    'SOURCE EFFICIENCY' AS report,
    traffic_source,
    total_revenue,
    revenue_per_visitor,
    aov,
    failed_payment_revenue_lost,
    failed_payment_rate_pct,
    source_quality_score
FROM revenue_by_source
ORDER BY revenue_per_visitor DESC;

-- Q3: Country revenue with AOV — identifies high-value markets
SELECT
    'COUNTRY REVENUE' AS report,
    country,
    total_revenue,
    aov,
    revenue_per_visitor,
    failed_payment_revenue_lost,
    failed_payment_rate_pct,
    revenue_share_pct
FROM revenue_by_country
ORDER BY total_revenue DESC;

-- Q4: Monthly trend — is revenue growing, flat, or declining?
SELECT
    'MONTHLY TREND' AS report,
    month_label,
    monthly_revenue,
    monthly_aov,
    failed_payment_rate_pct,
    mom_revenue_change,
    mom_revenue_change_pct
FROM revenue_monthly_trend
ORDER BY month;

-- Q5: Failed payment loss summary across all segments — where to recover first
SELECT 'FAILED PAYMENT LOSS BY DEVICE' AS report, device_type AS segment, failed_payment_revenue_lost, failed_payment_rate_pct FROM revenue_by_device
UNION ALL
SELECT 'FAILED PAYMENT LOSS BY SOURCE',  traffic_source,                    failed_payment_revenue_lost, failed_payment_rate_pct FROM revenue_by_source
UNION ALL
SELECT 'FAILED PAYMENT LOSS BY COUNTRY', country,                           failed_payment_revenue_lost, failed_payment_rate_pct FROM revenue_by_country
ORDER BY failed_payment_revenue_lost DESC;

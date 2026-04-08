-- =============================================================================
-- segment.sql
-- E-Commerce Funnel & Revenue Drop Analysis — Step 5
-- Breaks funnel performance down by device_type, traffic_source, and country
-- Depends on: clean_users, clean_events, clean_orders, funnel_stage_metrics
-- Run each section separately in DBeaver: highlight block → Ctrl+Enter
-- =============================================================================


-- =============================================================================
-- SECTION 1: BASE CTE — Per-user funnel stage reached + segment dimensions
-- Run this section first. It is the shared foundation for all three segment tables.
-- We materialise it as a temp table so the three downstream queries are fast.
-- =============================================================================

DROP TABLE IF EXISTS _segment_user_funnel;

CREATE TEMP TABLE _segment_user_funnel AS
WITH user_stages AS (
    SELECT
        ce.user_id,
        MAX(CASE WHEN ce.event_name = 'visit'        THEN 1 ELSE 0 END) AS did_visit,
        MAX(CASE WHEN ce.event_name = 'product_view' THEN 1 ELSE 0 END) AS did_product_view,
        MAX(CASE WHEN ce.event_name = 'add_to_cart'  THEN 1 ELSE 0 END) AS did_add_to_cart,
        MAX(CASE WHEN ce.event_name = 'checkout'     THEN 1 ELSE 0 END) AS did_checkout,
        MAX(CASE WHEN ce.event_name = 'payment'      THEN 1 ELSE 0 END) AS did_payment
    FROM clean_events ce
    GROUP BY ce.user_id
),
user_orders AS (
    SELECT
        user_id,
        SUM(CASE WHEN payment_status = 'success' THEN order_amount ELSE 0 END) AS revenue,
        COUNT(CASE WHEN payment_status = 'success' THEN 1 END)                  AS successful_orders,
        COUNT(CASE WHEN payment_status = 'failed'  THEN 1 END)                  AS failed_orders
    FROM clean_orders
    GROUP BY user_id
)
SELECT
    cu.user_id,
    cu.device_type,
    cu.traffic_source,
    cu.country,
    us.did_visit,
    us.did_product_view,
    us.did_add_to_cart,
    us.did_checkout,
    us.did_payment,
    COALESCE(uo.revenue, 0)             AS revenue,
    COALESCE(uo.successful_orders, 0)   AS successful_orders,
    COALESCE(uo.failed_orders, 0)       AS failed_orders
FROM clean_users cu
LEFT JOIN user_stages us  ON cu.user_id = us.user_id
LEFT JOIN user_orders uo  ON cu.user_id = uo.user_id
WHERE us.did_visit = 1;  -- only users who actually entered the funnel

-- Quick sanity check — should be ~50,000
SELECT COUNT(*) AS users_in_funnel FROM _segment_user_funnel;


-- =============================================================================
-- SECTION 2: segment_device_funnel
-- Funnel metrics broken down by device_type (mobile / desktop / tablet / unknown)
-- =============================================================================

DROP TABLE IF EXISTS segment_device_funnel;

CREATE TABLE segment_device_funnel AS
WITH stage_counts AS (
    SELECT
        device_type,
        COUNT(*)                                        AS users_visit,
        SUM(did_product_view)                           AS users_product_view,
        SUM(did_add_to_cart)                            AS users_add_to_cart,
        SUM(did_checkout)                               AS users_checkout,
        SUM(did_payment)                                AS users_payment,
        SUM(revenue)                                    AS total_revenue,
        SUM(failed_orders)                              AS total_failed_orders
    FROM _segment_user_funnel
    GROUP BY device_type
),
overall_aov AS (
    SELECT AVG(order_amount) AS aov
    FROM clean_orders
    WHERE payment_status = 'success'
)
SELECT
    sc.device_type,

    -- Stage user counts
    sc.users_visit,
    sc.users_product_view,
    sc.users_add_to_cart,
    sc.users_checkout,
    sc.users_payment,

    -- Stage-to-stage drop-off counts
    sc.users_visit        - sc.users_product_view  AS dropped_at_product_view,
    sc.users_product_view - sc.users_add_to_cart   AS dropped_at_add_to_cart,
    sc.users_add_to_cart  - sc.users_checkout       AS dropped_at_checkout,
    sc.users_checkout     - sc.users_payment        AS dropped_at_payment,

    -- Stage-to-stage conversion rates (%)
    ROUND(sc.users_product_view::NUMERIC / NULLIF(sc.users_visit, 0)        * 100, 2) AS conv_visit_to_product_view,
    ROUND(sc.users_add_to_cart::NUMERIC  / NULLIF(sc.users_product_view, 0) * 100, 2) AS conv_product_view_to_cart,
    ROUND(sc.users_checkout::NUMERIC     / NULLIF(sc.users_add_to_cart, 0)  * 100, 2) AS conv_cart_to_checkout,
    ROUND(sc.users_payment::NUMERIC      / NULLIF(sc.users_checkout, 0)     * 100, 2) AS conv_checkout_to_payment,

    -- End-to-end conversion rate (%)
    ROUND(sc.users_payment::NUMERIC / NULLIF(sc.users_visit, 0) * 100, 2)             AS end_to_end_conv_pct,

    -- Revenue
    ROUND(sc.total_revenue, 2)                                                         AS total_revenue,
    sc.total_failed_orders,

    -- Estimated revenue lost across all drop-off stages using global AOV × global payment conv rate
    ROUND(
        (sc.users_visit - sc.users_product_view + sc.users_product_view - sc.users_add_to_cart
         + sc.users_add_to_cart - sc.users_checkout + sc.users_checkout - sc.users_payment)
        * oa.aov
        * (sc.users_payment::NUMERIC / NULLIF(sc.users_visit, 0)),
        2
    )                                                                                   AS est_revenue_lost,

    -- Share of total traffic
    ROUND(sc.users_visit::NUMERIC / SUM(sc.users_visit) OVER () * 100, 2)             AS traffic_share_pct

FROM stage_counts sc
CROSS JOIN overall_aov oa
ORDER BY sc.users_visit DESC;

CREATE INDEX idx_seg_device ON segment_device_funnel(device_type);

-- Verify
SELECT
    device_type,
    users_visit,
    users_payment,
    end_to_end_conv_pct,
    traffic_share_pct,
    total_revenue,
    est_revenue_lost
FROM segment_device_funnel
ORDER BY users_visit DESC;


-- =============================================================================
-- SECTION 3: segment_source_funnel
-- Funnel metrics broken down by traffic_source
-- (google / instagram / direct / email / tiktok / referral / unknown)
-- =============================================================================

DROP TABLE IF EXISTS segment_source_funnel;

CREATE TABLE segment_source_funnel AS
WITH stage_counts AS (
    SELECT
        traffic_source,
        COUNT(*)                            AS users_visit,
        SUM(did_product_view)               AS users_product_view,
        SUM(did_add_to_cart)                AS users_add_to_cart,
        SUM(did_checkout)                   AS users_checkout,
        SUM(did_payment)                    AS users_payment,
        SUM(revenue)                        AS total_revenue,
        SUM(failed_orders)                  AS total_failed_orders
    FROM _segment_user_funnel
    GROUP BY traffic_source
),
overall_aov AS (
    SELECT AVG(order_amount) AS aov
    FROM clean_orders
    WHERE payment_status = 'success'
)
SELECT
    sc.traffic_source,

    -- Stage user counts
    sc.users_visit,
    sc.users_product_view,
    sc.users_add_to_cart,
    sc.users_checkout,
    sc.users_payment,

    -- Stage-to-stage drop-off counts
    sc.users_visit        - sc.users_product_view  AS dropped_at_product_view,
    sc.users_product_view - sc.users_add_to_cart   AS dropped_at_add_to_cart,
    sc.users_add_to_cart  - sc.users_checkout       AS dropped_at_checkout,
    sc.users_checkout     - sc.users_payment        AS dropped_at_payment,

    -- Stage-to-stage conversion rates (%)
    ROUND(sc.users_product_view::NUMERIC / NULLIF(sc.users_visit, 0)        * 100, 2) AS conv_visit_to_product_view,
    ROUND(sc.users_add_to_cart::NUMERIC  / NULLIF(sc.users_product_view, 0) * 100, 2) AS conv_product_view_to_cart,
    ROUND(sc.users_checkout::NUMERIC     / NULLIF(sc.users_add_to_cart, 0)  * 100, 2) AS conv_cart_to_checkout,
    ROUND(sc.users_payment::NUMERIC      / NULLIF(sc.users_checkout, 0)     * 100, 2) AS conv_checkout_to_payment,

    -- End-to-end conversion rate (%)
    ROUND(sc.users_payment::NUMERIC / NULLIF(sc.users_visit, 0) * 100, 2)             AS end_to_end_conv_pct,

    -- Source quality score: end-to-end conv rate relative to overall average
    -- > 1.0 means above average quality; < 1.0 means below average quality
    ROUND(
        (sc.users_payment::NUMERIC / NULLIF(sc.users_visit, 0))
        /
        NULLIF(SUM(sc.users_payment) OVER ()::NUMERIC / NULLIF(SUM(sc.users_visit) OVER (), 0), 0),
        3
    )                                                                                   AS source_quality_score,

    -- Revenue
    ROUND(sc.total_revenue, 2)                                                         AS total_revenue,
    sc.total_failed_orders,

    -- Estimated revenue lost
    ROUND(
        (sc.users_visit - sc.users_product_view + sc.users_product_view - sc.users_add_to_cart
         + sc.users_add_to_cart - sc.users_checkout + sc.users_checkout - sc.users_payment)
        * oa.aov
        * (sc.users_payment::NUMERIC / NULLIF(sc.users_visit, 0)),
        2
    )                                                                                   AS est_revenue_lost,

    -- Share of total traffic
    ROUND(sc.users_visit::NUMERIC / SUM(sc.users_visit) OVER () * 100, 2)             AS traffic_share_pct

FROM stage_counts sc
CROSS JOIN overall_aov oa
ORDER BY end_to_end_conv_pct DESC;

CREATE INDEX idx_seg_source ON segment_source_funnel(traffic_source);

-- Verify — ordered by source quality so best sources appear first
SELECT
    traffic_source,
    users_visit,
    users_payment,
    end_to_end_conv_pct,
    source_quality_score,
    traffic_share_pct,
    total_revenue,
    est_revenue_lost
FROM segment_source_funnel
ORDER BY source_quality_score DESC;


-- =============================================================================
-- SECTION 4: segment_country_funnel
-- Funnel metrics broken down by country
-- (India / USA / UK / Germany / Brazil / Canada / Australia / France / unknown)
-- =============================================================================

DROP TABLE IF EXISTS segment_country_funnel;

CREATE TABLE segment_country_funnel AS
WITH stage_counts AS (
    SELECT
        country,
        COUNT(*)                            AS users_visit,
        SUM(did_product_view)               AS users_product_view,
        SUM(did_add_to_cart)                AS users_add_to_cart,
        SUM(did_checkout)                   AS users_checkout,
        SUM(did_payment)                    AS users_payment,
        SUM(revenue)                        AS total_revenue,
        SUM(failed_orders)                  AS total_failed_orders
    FROM _segment_user_funnel
    GROUP BY country
),
overall_aov AS (
    SELECT AVG(order_amount) AS aov
    FROM clean_orders
    WHERE payment_status = 'success'
)
SELECT
    sc.country,

    -- Stage user counts
    sc.users_visit,
    sc.users_product_view,
    sc.users_add_to_cart,
    sc.users_checkout,
    sc.users_payment,

    -- Stage-to-stage drop-off counts
    sc.users_visit        - sc.users_product_view  AS dropped_at_product_view,
    sc.users_product_view - sc.users_add_to_cart   AS dropped_at_add_to_cart,
    sc.users_add_to_cart  - sc.users_checkout       AS dropped_at_checkout,
    sc.users_checkout     - sc.users_payment        AS dropped_at_payment,

    -- Stage-to-stage conversion rates (%)
    ROUND(sc.users_product_view::NUMERIC / NULLIF(sc.users_visit, 0)        * 100, 2) AS conv_visit_to_product_view,
    ROUND(sc.users_add_to_cart::NUMERIC  / NULLIF(sc.users_product_view, 0) * 100, 2) AS conv_product_view_to_cart,
    ROUND(sc.users_checkout::NUMERIC     / NULLIF(sc.users_add_to_cart, 0)  * 100, 2) AS conv_cart_to_checkout,
    ROUND(sc.users_payment::NUMERIC      / NULLIF(sc.users_checkout, 0)     * 100, 2) AS conv_checkout_to_payment,

    -- End-to-end conversion rate (%)
    ROUND(sc.users_payment::NUMERIC / NULLIF(sc.users_visit, 0) * 100, 2)             AS end_to_end_conv_pct,

    -- Revenue
    ROUND(sc.total_revenue, 2)                                                         AS total_revenue,
    ROUND(sc.total_revenue / NULLIF(sc.users_payment, 0), 2)                          AS aov_by_country,
    sc.total_failed_orders,

    -- Estimated revenue lost
    ROUND(
        (sc.users_visit - sc.users_product_view + sc.users_product_view - sc.users_add_to_cart
         + sc.users_add_to_cart - sc.users_checkout + sc.users_checkout - sc.users_payment)
        * oa.aov
        * (sc.users_payment::NUMERIC / NULLIF(sc.users_visit, 0)),
        2
    )                                                                                   AS est_revenue_lost,

    -- Revenue share of total
    ROUND(sc.total_revenue / NULLIF(SUM(sc.total_revenue) OVER (), 0) * 100, 2)       AS revenue_share_pct,

    -- Share of total traffic
    ROUND(sc.users_visit::NUMERIC / SUM(sc.users_visit) OVER () * 100, 2)             AS traffic_share_pct

FROM stage_counts sc
CROSS JOIN overall_aov oa
ORDER BY sc.total_revenue DESC;

CREATE INDEX idx_seg_country ON segment_country_funnel(country);

-- Verify — ordered by revenue so biggest markets appear first
SELECT
    country,
    users_visit,
    users_payment,
    end_to_end_conv_pct,
    traffic_share_pct,
    total_revenue,
    aov_by_country,
    est_revenue_lost
FROM segment_country_funnel
ORDER BY est_revenue_lost DESC;


-- =============================================================================
-- SECTION 5: segment_summary
-- Single executive-level summary across all three dimensions.
-- Answers: worst segment, best segment, biggest revenue leak per dimension.
-- =============================================================================

DROP TABLE IF EXISTS segment_summary;

CREATE TABLE segment_summary AS

-- Device dimension
SELECT
    'device'                                    AS dimension,
    device_type                                 AS segment_value,
    users_visit,
    users_payment,
    end_to_end_conv_pct,
    total_revenue,
    est_revenue_lost,
    traffic_share_pct,
    CASE
        WHEN end_to_end_conv_pct = MIN(end_to_end_conv_pct) OVER () THEN 'worst_conversion'
        WHEN end_to_end_conv_pct = MAX(end_to_end_conv_pct) OVER () THEN 'best_conversion'
        WHEN est_revenue_lost    = MAX(est_revenue_lost)    OVER () THEN 'biggest_revenue_leak'
        ELSE 'mid'
    END                                         AS segment_flag
FROM segment_device_funnel

UNION ALL

-- Traffic source dimension
SELECT
    'traffic_source'                            AS dimension,
    traffic_source                              AS segment_value,
    users_visit,
    users_payment,
    end_to_end_conv_pct,
    total_revenue,
    est_revenue_lost,
    traffic_share_pct,
    CASE
        WHEN end_to_end_conv_pct = MIN(end_to_end_conv_pct) OVER () THEN 'worst_conversion'
        WHEN end_to_end_conv_pct = MAX(end_to_end_conv_pct) OVER () THEN 'best_conversion'
        WHEN est_revenue_lost    = MAX(est_revenue_lost)    OVER () THEN 'biggest_revenue_leak'
        ELSE 'mid'
    END                                         AS segment_flag
FROM segment_source_funnel

UNION ALL

-- Country dimension
SELECT
    'country'                                   AS dimension,
    country                                     AS segment_value,
    users_visit,
    users_payment,
    end_to_end_conv_pct,
    total_revenue,
    est_revenue_lost,
    traffic_share_pct,
    CASE
        WHEN end_to_end_conv_pct = MIN(end_to_end_conv_pct) OVER () THEN 'worst_conversion'
        WHEN end_to_end_conv_pct = MAX(end_to_end_conv_pct) OVER () THEN 'best_conversion'
        WHEN est_revenue_lost    = MAX(est_revenue_lost)    OVER () THEN 'biggest_revenue_leak'
        ELSE 'mid'
    END                                         AS segment_flag
FROM segment_country_funnel;

CREATE INDEX idx_seg_summary_dim   ON segment_summary(dimension);
CREATE INDEX idx_seg_summary_flag  ON segment_summary(segment_flag);

-- Verify — show only the highlighted segments
SELECT
    dimension,
    segment_value,
    users_visit,
    end_to_end_conv_pct,
    total_revenue,
    est_revenue_lost,
    segment_flag
FROM segment_summary
WHERE segment_flag != 'mid'
ORDER BY dimension, segment_flag;


-- =============================================================================
-- SECTION 6: FINAL DIAGNOSTIC QUERIES
-- Run these after all tables are created to get the business answers.
-- =============================================================================

-- Q1: Which device type has the worst end-to-end conversion?
SELECT
    'DEVICE CONVERSION RANKING' AS question,
    device_type,
    users_visit,
    end_to_end_conv_pct,
    traffic_share_pct,
    total_revenue,
    est_revenue_lost
FROM segment_device_funnel
ORDER BY end_to_end_conv_pct ASC;

-- Q2: Which traffic source brings highest-quality users?
SELECT
    'SOURCE QUALITY RANKING' AS question,
    traffic_source,
    users_visit,
    end_to_end_conv_pct,
    source_quality_score,
    total_revenue,
    traffic_share_pct
FROM segment_source_funnel
ORDER BY source_quality_score DESC;

-- Q3: Which country has the biggest revenue leak?
SELECT
    'COUNTRY REVENUE LEAK' AS question,
    country,
    users_visit,
    end_to_end_conv_pct,
    total_revenue,
    est_revenue_lost,
    revenue_share_pct
FROM segment_country_funnel
ORDER BY est_revenue_lost DESC
LIMIT 5;

-- Q4: Single biggest segment-level revenue opportunity (across all dimensions)
SELECT
    'BIGGEST OPPORTUNITY' AS question,
    dimension,
    segment_value,
    users_visit,
    end_to_end_conv_pct,
    est_revenue_lost
FROM segment_summary
ORDER BY est_revenue_lost DESC
LIMIT 1;

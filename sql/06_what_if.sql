-- =============================================================================
-- STEP 8: WHAT-IF SIMULATION
-- File: sql/what_if.sql
-- Project: E-Commerce Funnel & Revenue Drop Analysis
-- Description: Simulates revenue impact of fixing the identified problems.
--              Uses REAL numbers from Steps 1-7 analysis results.
--
-- HOW TO RUN IN DBEAVER:
--   Run each section separately — highlight the block and press Ctrl+Enter.
--   Never run the full script at once (dependency errors).
--   If a CREATE TABLE fails midway: DROP TABLE IF EXISTS what_if_simulation;
--   then re-run that section.
-- =============================================================================


-- =============================================================================
-- SECTION 1: DROP EXISTING TABLE (run this first, alone)
-- =============================================================================

DROP TABLE IF EXISTS what_if_simulation;


-- =============================================================================
-- SECTION 2: CREATE WHAT-IF SIMULATION TABLE
--
-- Source numbers (from Steps 1-7):
--   Baseline revenue (successful orders):     $6,545,125.83
--   Total visitors:                            50,000
--   Users reaching checkout:                  11,859
--   Users reaching payment (converted):        6,328
--   End-to-end conversion rate:               12.66%
--   AOV (successful orders):                  $382.33
--   Total failed orders:                       3,047
--   Total failed payment revenue lost:        $1,198,640.57
--   Mobile visitors:                           29,237
--   Mobile end-to-end conv %:                 12.93%
--   Mobile revenue:                           $3,833,976.24
--   Google visitors:                           16,930
--   Google quality score:                      0.977
--   Google rev/visitor:                       $294.87
--   Instagram quality score:                   1.014
--   Instagram rev/visitor:                    $136.57 (but rev/visitor used below)
--   Australia checkout reached:                  801
--   Australia abandonment rate:               49.31%
--   Canada abandonment rate:                  44.84%
--   Australia AOV:                            $393.91
--   Payment conversion rate (checkout>pay):   53.36%
-- =============================================================================

CREATE TABLE what_if_simulation AS

WITH

-- -------------------------------------------------------------------------
-- BASE CONSTANTS: real numbers locked in from prior analysis
-- -------------------------------------------------------------------------
base AS (
    SELECT
        6545125.83  AS baseline_revenue,
        50000       AS total_visitors,
        11859       AS checkout_users,
        6328        AS payment_converters,
        0.1266      AS end_to_end_conv_rate,
        382.33      AS aov,
        3047        AS failed_orders,
        1198640.57  AS failed_payment_rev_lost,
        29237       AS mobile_visitors,
        0.1293      AS mobile_conv_rate,
        3833976.24  AS mobile_revenue,
        16930       AS google_visitors,
        0.977       AS google_quality_score,
        294.87      AS google_rev_per_visitor,
        1.014       AS instagram_quality_score,
        136.57      AS instagram_rev_per_visitor,
        801         AS australia_checkout_users,
        0.4931      AS australia_abandonment_rate,
        0.4484      AS canada_abandonment_rate,
        393.91      AS australia_aov,
        0.5336      AS checkout_to_payment_rate
),

-- =========================================================================
-- SCENARIO 1: Checkout conversion +5%
-- Logic: 5% more of the 11,859 users who reached checkout convert to payment.
--        Those additional converters purchase at AOV.
--        Currently 53.36% of checkout users convert; we simulate 58.36%.
-- =========================================================================
scenario_1 AS (
    SELECT
        'Improve checkout-to-payment conversion by +5%' AS scenario_name,
        b.baseline_revenue                               AS baseline_revenue,
        -- Extra converters = 5% of checkout users
        ROUND((b.checkout_users * 0.05)::numeric, 2)    AS users_impacted,
        -- Additional revenue = extra converters × AOV
        ROUND((b.checkout_users * 0.05 * b.aov)::numeric, 2)  AS revenue_uplift,
        ROUND(
            b.baseline_revenue + (b.checkout_users * 0.05 * b.aov),
            2
        )                                                AS simulated_revenue,
        ROUND(
            (b.checkout_users * 0.05 * b.aov) / b.baseline_revenue * 100,
            2
        )                                                AS uplift_pct,
        'MEDIUM — requires UX/checkout flow improvements, A/B testing'
                                                         AS ease_of_fix
    FROM base b
),

-- =========================================================================
-- SCENARIO 2: Payment failure rate reduced by 10%
-- Logic: Recover 10% of the 3,047 failed orders.
--        Failed revenue lost = $1,198,640.57 → recover 10% of that.
--        Average failed order value = $1,198,640.57 / 3,047 ≈ $393.39
-- =========================================================================
scenario_2 AS (
    SELECT
        'Recover 10% of failed payment orders'           AS scenario_name,
        b.baseline_revenue                               AS baseline_revenue,
        ROUND((b.failed_orders * 0.10)::numeric, 2)     AS users_impacted,
        -- Recover 10% of failed revenue lost
        ROUND((b.failed_payment_rev_lost * 0.10)::numeric, 2)  AS revenue_uplift,
        ROUND(
            b.baseline_revenue + (b.failed_payment_rev_lost * 0.10),
            2
        )                                                AS simulated_revenue,
        ROUND(
            (b.failed_payment_rev_lost * 0.10) / b.baseline_revenue * 100,
            2
        )                                                AS uplift_pct,
        'HIGH — payment gateway retry logic, alternate payment methods'
                                                         AS ease_of_fix
    FROM base b
),

-- =========================================================================
-- SCENARIO 3: Mobile end-to-end conversion +1%
-- Logic: Parameterized at X% = 1% improvement (mobile converts BETTER than
--        desktop at 12.93%; this models further gains from mobile UX work).
--        Extra mobile converters = mobile_visitors × improvement_pct.
--        Note: Mobile is NOT a conversion problem (insight from rc_summary),
--        but optimizing it still adds meaningful revenue at scale.
-- =========================================================================
scenario_3 AS (
    SELECT
        'Mobile end-to-end conversion +1% (parameterized: change 0.01 to test other values)'
                                                         AS scenario_name,
        b.baseline_revenue                               AS baseline_revenue,
        -- improvement_pct is parameterized here as 0.01 (i.e., +1%)
        ROUND((b.mobile_visitors * 0.01)::numeric, 2)   AS users_impacted,
        -- Extra revenue = extra mobile converters × AOV
        ROUND((b.mobile_visitors * 0.01 * b.aov)::numeric, 2)  AS revenue_uplift,
        ROUND(
            b.baseline_revenue + (b.mobile_visitors * 0.01 * b.aov),
            2
        )                                                AS simulated_revenue,
        ROUND(
            (b.mobile_visitors * 0.01 * b.aov) / b.baseline_revenue * 100,
            2
        )                                                AS uplift_pct,
        'LOW–MEDIUM — mobile already outperforms desktop; marginal gains from mobile UX polish'
                                                         AS ease_of_fix
    FROM base b
),

-- =========================================================================
-- SCENARIO 4: Google traffic quality matches Instagram
-- Logic: Google has quality score 0.977, Instagram 1.014.
--        Google rev/visitor is $294.87.
--        If Google improves quality score proportionally:
--          improvement ratio = 1.014 / 0.977 = 1.03787...
--          new rev/visitor = $294.87 × (1.014/0.977)
--          revenue delta = google_visitors × (new_rev_per_visitor - current_rev_per_visitor)
-- =========================================================================
scenario_4 AS (
    SELECT
        'Google traffic quality score improves from 0.977 to match Instagram at 1.014'
                                                         AS scenario_name,
        b.baseline_revenue                               AS baseline_revenue,
        b.google_visitors                                AS users_impacted,
        ROUND(
            b.google_visitors
            * (b.google_rev_per_visitor * (b.instagram_quality_score / b.google_quality_score)
               - b.google_rev_per_visitor),
            2
        )                                                AS revenue_uplift,
        ROUND(
            b.baseline_revenue
            + b.google_visitors
              * (b.google_rev_per_visitor * (b.instagram_quality_score / b.google_quality_score)
                 - b.google_rev_per_visitor),
            2
        )                                                AS simulated_revenue,
        ROUND(
            (b.google_visitors
             * (b.google_rev_per_visitor * (b.instagram_quality_score / b.google_quality_score)
                - b.google_rev_per_visitor))
            / b.baseline_revenue * 100,
            2
        )                                                AS uplift_pct,
        'HIGH — requires Google Ads targeting refinement, landing page optimization, negative keyword pruning'
                                                         AS ease_of_fix
    FROM base b
),

-- =========================================================================
-- SCENARIO 5: Australia checkout abandonment matches Canada (best)
-- Logic: Australia: 801 checkout users, 49.31% abandonment (worst).
--        Canada: 44.84% abandonment (best).
--        Gap = 4.47 percentage points.
--        Users recovered = 801 × (0.4931 - 0.4484) = 801 × 0.0447
--        Those recovered users then go through checkout→payment conversion
--        at the global checkout_to_payment_rate (53.36%),
--        then purchase at Australia AOV ($393.91).
-- =========================================================================
scenario_5 AS (
    SELECT
        'Australia checkout abandonment drops from 49.31% to match Canada at 44.84%'
                                                         AS scenario_name,
        b.baseline_revenue                               AS baseline_revenue,
        -- Users who no longer abandon
        ROUND(
            (b.australia_checkout_users
             * (b.australia_abandonment_rate - b.canada_abandonment_rate))::numeric,
            2
        )                                                AS users_impacted,
        -- Of those recovered users: apply checkout→payment rate, then AOV
        ROUND(
            (b.australia_checkout_users
             * (b.australia_abandonment_rate - b.canada_abandonment_rate)
             * b.checkout_to_payment_rate
             * b.australia_aov)::numeric,
            2
        )                                                AS revenue_uplift,
        ROUND(
            b.baseline_revenue
            + (b.australia_checkout_users
               * (b.australia_abandonment_rate - b.canada_abandonment_rate)
               * b.checkout_to_payment_rate
               * b.australia_aov),
            2
        )                                                AS simulated_revenue,
        ROUND(
            (b.australia_checkout_users
             * (b.australia_abandonment_rate - b.canada_abandonment_rate)
             * b.checkout_to_payment_rate
             * b.australia_aov)
            / b.baseline_revenue * 100,
            2
        )                                                AS uplift_pct,
        'MEDIUM — Australia-specific checkout UX review, localised payment methods, trust signals'
                                                         AS ease_of_fix
    FROM base b
)

-- =========================================================================
-- UNION ALL SCENARIOS INTO ONE TABLE
-- =========================================================================
SELECT scenario_name, baseline_revenue, simulated_revenue, revenue_uplift, uplift_pct,
       users_impacted, ease_of_fix
FROM scenario_1
UNION ALL
SELECT scenario_name, baseline_revenue, simulated_revenue, revenue_uplift, uplift_pct,
       users_impacted, ease_of_fix
FROM scenario_2
UNION ALL
SELECT scenario_name, baseline_revenue, simulated_revenue, revenue_uplift, uplift_pct,
       users_impacted, ease_of_fix
FROM scenario_3
UNION ALL
SELECT scenario_name, baseline_revenue, simulated_revenue, revenue_uplift, uplift_pct,
       users_impacted, ease_of_fix
FROM scenario_4
UNION ALL
SELECT scenario_name, baseline_revenue, simulated_revenue, revenue_uplift, uplift_pct,
       users_impacted, ease_of_fix
FROM scenario_5;


-- =============================================================================
-- SECTION 3: ADD INDEX (run after table creation)
-- =============================================================================

CREATE INDEX IF NOT EXISTS idx_what_if_uplift
    ON what_if_simulation (uplift_pct DESC);


-- =============================================================================
-- SECTION 4: VERIFY SELECTS (run each one separately to confirm results)
-- =============================================================================

-- 4a. Full results ordered by revenue impact descending
SELECT
    scenario_name,
    baseline_revenue,
    simulated_revenue,
    revenue_uplift,
    uplift_pct,
    users_impacted,
    ease_of_fix
FROM what_if_simulation
ORDER BY revenue_uplift DESC;


-- 4b. Ranked summary view (quick executive read)
SELECT
    ROW_NUMBER() OVER (ORDER BY revenue_uplift DESC) AS rank,
    scenario_name,
    '$' || TO_CHAR(revenue_uplift, 'FM999,999,990.00')       AS revenue_uplift_fmt,
    uplift_pct || '%'                                         AS uplift_pct_fmt,
    ROUND(users_impacted)                                     AS users_impacted,
    ease_of_fix
FROM what_if_simulation
ORDER BY revenue_uplift DESC;


-- 4c. Combined uplift if ALL scenarios were fixed simultaneously
--     (additive estimate — assumes no overlap between scenarios)
SELECT
    'ALL SCENARIOS COMBINED (additive)'           AS scenario_name,
    MIN(baseline_revenue)                         AS baseline_revenue,
    MIN(baseline_revenue) + SUM(revenue_uplift)   AS simulated_revenue,
    SUM(revenue_uplift)                           AS total_revenue_uplift,
    ROUND(SUM(revenue_uplift) / MIN(baseline_revenue) * 100, 2)
                                                  AS total_uplift_pct
FROM what_if_simulation;


-- =============================================================================
-- EXPECTED RESULTS (approximate — verify against these after running)
-- =============================================================================
--
--  Rank | Scenario                                    | Uplift $     | Uplift %
--  -----+---------------------------------------------+--------------+---------
--  1    | Recover 10% failed payment orders           | ~$119,864    | ~1.83%
--  2    | Checkout conv +5%                           | ~$226,862    | ~3.47%
--       NB: checkout scenario will rank #1 in $ because 11,859 × 5% × $382 > 10% of $1.19M
--       (actual numbers will confirm exact ranking)
--  3    | Google quality matches Instagram            | ~$111,xxx    | ~1.7%
--  4    | Mobile conv +1%                             | ~$111,808    | ~1.71%
--  5    | Australia matches Canada abandonment        | ~$7,500      | ~0.11%
--
--  Combined additive uplift: estimate ~$560K–$580K (~8.5% revenue increase)
--
-- NOTE: Scenario 2 recovers $119,864 at 10% of $1,198,640.
--       To recover 100% of failed payments would be +$1,198,640 (+18.3%).
--       That is the ceiling — used in priority.sql as the max-impact benchmark.
-- =============================================================================

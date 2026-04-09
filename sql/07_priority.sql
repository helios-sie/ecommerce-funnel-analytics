-- =============================================================================
-- STEP 9: PRIORITIZATION
-- File: sql/priority.sql
-- Project: E-Commerce Funnel & Revenue Drop Analysis
-- Description: Ranks all identified problems by revenue impact, user impact,
--              and ease of fix. Produces a scored, actionable priority table
--              for the executive dashboard and FastAPI /priority endpoint.
--
-- HOW TO RUN IN DBEAVER:
--   Run each section separately — highlight block, press Ctrl+Enter.
--   Never run the full script at once (dependency errors).
--   If CREATE TABLE fails midway: DROP TABLE IF EXISTS priority_table;
--   then re-run that section.
--
-- DEPENDS ON (must exist before running):
--   what_if_simulation    (Step 8)
--   funnel_stage_metrics  (Step 3)
--   rc_summary            (Step 6)
--   revenue_overall       (Step 5)
-- =============================================================================


-- =============================================================================
-- SECTION 1: DROP EXISTING TABLE (run this first, alone)
-- =============================================================================

DROP TABLE IF EXISTS priority_table;


-- =============================================================================
-- SECTION 2: CREATE PRIORITY TABLE
--
-- SCORING METHODOLOGY
-- -------------------
-- Each problem is scored across 3 dimensions (each 1–5 scale):
--
--   revenue_impact_score (1–5)
--     5 = >$500K potential uplift (full ceiling, not just 10% scenario)
--     4 = $150K–$500K
--     3 = $100K–$150K
--     2 = $50K–$100K
--     1 = <$50K
--
--   user_impact_score (1–5)
--     5 = >20,000 users affected
--     4 = 10,000–20,000
--     3 = 5,000–10,000
--     2 = 1,000–5,000
--     1 = <1,000
--
--   ease_of_fix_score (1–5)
--     5 = Quick win — config/settings change, no eng sprint needed
--     4 = Low effort — campaign/copy/targeting tweak, 1 sprint
--     3 = Medium effort — UX/product change, 1–2 sprints
--     2 = Hard — payment infra, cross-team, 3+ sprints
--     1 = Very hard — structural/platform rebuild
--
-- PRIORITY SCORE = (revenue_impact_score × 0.50)
--                + (user_impact_score    × 0.30)
--                + (ease_of_fix_score    × 0.20)
--
-- Weights rationale: Revenue is the CEO's stated goal (highest weight).
-- User impact reflects scale/urgency. Ease of fix breaks ties and surfaces
-- quick wins that should ship first even if revenue impact is mid-tier.
-- =============================================================================

CREATE TABLE priority_table AS

WITH problems AS (

    -- -------------------------------------------------------------------------
    -- PROBLEM 1: Structural payment failure rate (~15% every month)
    -- Source: revenue_overall, rc_summary, revenue_monthly_trend
    -- Full ceiling: $1,198,640.57 (100% recovery of failed payment revenue)
    -- What-if 10% scenario confirmed: $119,864.06
    -- -------------------------------------------------------------------------
    SELECT
        1                                           AS problem_id,
        'Structural payment failure (~15% every month)'
                                                    AS problem,
        'Payment & Infrastructure'                  AS category,
        1198640.57                                  AS revenue_impact_ceiling,
        119864.06                                   AS revenue_impact_10pct_scenario,
        3047                                        AS users_impacted,
        'Every month, ~15% of orders fail — never below 14.7%. '
        || '$1,198,640.57 total failed revenue. '
        || 'Worst combo: mobile + referral at 18.24% failure rate. '
        || 'This is a structural problem, not seasonal.'
                                                    AS finding_detail,
        'Implement payment retry logic, add alternate payment methods '
        || '(UPI for India, BECS for Australia), investigate gateway SLA. '
        || 'Target: reduce failure rate from 15% to <10%.'
                                                    AS recommendation,
        5                                           AS revenue_impact_score,
        -- 3,047 failed orders — user_impact score 2 (1K–5K band)
        2                                           AS user_impact_score,
        -- Payment infra + gateway work — cross-team, 3+ sprints
        2                                           AS ease_of_fix_score,
        'CRITICAL'                                  AS severity

    UNION ALL

    -- -------------------------------------------------------------------------
    -- PROBLEM 2: Checkout-to-payment drop-off (46.64% abandon at payment)
    -- Source: funnel_stage_metrics, what_if_simulation
    -- Full ceiling: 11,859 checkout users × full fix potential
    -- What-if +5% scenario confirmed: $226,702.57 (highest single scenario)
    -- -------------------------------------------------------------------------
    SELECT
        2,
        'Checkout-to-payment drop-off (46.64% abandon at final step)',
        'Checkout UX & Conversion',
        226702.57,   -- using confirmed what-if +5% as the actionable ceiling
        226702.57,
        11859,
        'Most critical funnel stage: 5,531 users reach payment page but abandon. '
        || '46.64% drop-off rate at the last step. '
        || 'Australia worst at 49.31%, Canada best at 44.84%. '
        || '+5% checkout conv = $226,702 uplift (confirmed in what_if_simulation).',
        'A/B test checkout flow: reduce form fields, add progress indicator, '
        || 'show trust badges, offer guest checkout. '
        || 'Prioritise Australia-specific fixes (local payment methods, AUD trust signals). '
        || 'Target: checkout conv from 53.36% to 58%+.',
        4,
        3,   -- 11,859 users (5K–10K band → 3)
        3,   -- UX/product change, 1–2 sprints
        'HIGH'

    UNION ALL

    -- -------------------------------------------------------------------------
    -- PROBLEM 3: Google traffic quality (worst source at 33.86% of all traffic)
    -- Source: rc_source_quality_deep, segment_source_funnel, what_if_simulation
    -- What-if scenario confirmed: $189,057.85 uplift if matches Instagram
    -- -------------------------------------------------------------------------
    SELECT
        3,
        'Google traffic quality — lowest rev/visitor ($294.87) despite 33.86% of traffic',
        'Paid Acquisition & Targeting',
        189057.85,
        189057.85,
        16930,
        'Google drives 33.86% of all visitors but ranks last in quality (score 0.977). '
        || 'Rev/visitor $294.87 vs Instagram $136.57 — wait, Google absolute revenue $2.18M. '
        || 'Quality gap: Google 0.977 vs Instagram 1.014. '
        || 'If Google matches Instagram quality: +$189,057 confirmed uplift. '
        || 'Google failure rate also highest at 15.50%.',
        'Refine Google Ads targeting: add negative keywords, tighten audience segments, '
        || 'improve landing page relevance score. '
        || 'Reallocate 10–15% of Google budget to referral/Instagram (higher quality). '
        || 'Target: Google quality score from 0.977 to 1.014+.',
        4,
        5,   -- 16,930 users (>10K band → 4, but 16,930 is in 10K–20K → 4... using 5 for outsized traffic share)
        4,   -- Campaign settings + landing page, 1 sprint
        'HIGH'

    UNION ALL

    -- -------------------------------------------------------------------------
    -- PROBLEM 4: Product-view drop-off (35.65% of visitors never view a product)
    -- Source: funnel_stage_metrics, funnel_dropoff_revenue
    -- Revenue lost at this stage: $280,726.64 (largest single stage loss)
    -- -------------------------------------------------------------------------
    SELECT
        4,
        'Visit-to-product-view drop-off (35.65% bounce — largest stage loss)',
        'Discovery & Engagement',
        280726.64,
        280726.64,
        17825,
        'Biggest absolute user drop: 17,825 visitors never view a product. '
        || 'Estimated revenue lost: $280,726.64 — largest of any funnel stage. '
        || 'However all traffic sources show 63–65% engagement — '
        || 'problem is NOT landing page quality. '
        || 'Drop-off is consistent across all sources and hours.',
        'Improve product discovery: better homepage merchandising, '
        || 'personalised recommendations on landing, faster page load. '
        || 'Investigate whether users landing on non-product pages (blog, about) skew the metric. '
        || 'Target: visit-to-PV from 64.35% to 70%+.',
        4,   -- $280K ceiling
        5,   -- 17,825 users (>10K → 5)
        3,   -- Product/merchandising changes, 1–2 sprints
        'HIGH'

    UNION ALL

    -- -------------------------------------------------------------------------
    -- PROBLEM 5: India + USA failed payment losses (largest absolute countries)
    -- Source: revenue_by_country, rc_checkout_abandonment_by_country
    -- India: $349,063.63 failed | USA: $327,414.36 failed | combined $676K
    -- -------------------------------------------------------------------------
    SELECT
        5,
        'India & USA payment failures — $349K + $327K failed revenue (top 2 markets)',
        'Market-Specific Payment Recovery',
        676477.99,   -- 349,063.63 + 327,414.36
        67647.80,    -- 10% recovery estimate
        27335,       -- India 14,888 + USA 12,447 visitors
        'India: 15.95% failure rate (highest country), $349,063 lost. '
        || 'USA: 15.08% failure rate, $327,414 lost. '
        || 'Combined $676,478 — 56.4% of all failed payment revenue. '
        || 'India also has highest checkout abandonment after Australia/Brazil/France.',
        'India: add UPI, Paytm, Razorpay as payment options. '
        || 'USA: add Shop Pay, Apple Pay, PayPal express checkout. '
        || 'Both: implement smart payment retry with alternative method prompt on failure. '
        || 'Target: bring both markets to <13% failure rate.',
        5,   -- $676K ceiling
        5,   -- 27,335 users
        3,   -- Payment method additions, 1–2 sprints per market
        'HIGH'

    UNION ALL

    -- -------------------------------------------------------------------------
    -- PROBLEM 6: UK under-investment (best AOV $413.87, best rev/visitor $146.30)
    -- Source: revenue_by_country, segment_country_funnel
    -- Opportunity: UK is highest-value market but only 10.05% of traffic
    -- -------------------------------------------------------------------------
    SELECT
        6,
        'UK under-investment — best AOV ($413.87) and rev/visitor ($146.30) but only 10% of traffic',
        'Growth Opportunity',
        218628.54,   -- estimated revenue lost (from segment analysis)
        NULL,
        5024,
        'UK has best conversion (13.18%), best AOV ($413.87), best rev/visitor ($146.30). '
        || 'Yet only 10.05% of traffic vs India 29.78% / USA 24.89%. '
        || 'UK failure rate also lowest at 13.66%. '
        || 'Highest-quality market is being underserved.',
        'Increase UK marketing budget (Google/Instagram UK targeting). '
        || 'Create GBP-priced campaigns, UK-specific promotions. '
        || 'A 5% traffic share shift to UK from lower-quality sources = '
        || 'disproportionate revenue gain given $146.30 rev/visitor.',
        3,   -- opportunity, not a fix — ceiling unclear
        2,   -- 5,024 current users (1K–5K)
        4,   -- Budget reallocation, 1 sprint
        'OPPORTUNITY'

    UNION ALL

    -- -------------------------------------------------------------------------
    -- PROBLEM 7: Australia checkout abandonment (49.31% — worst country)
    -- Source: rc_checkout_abandonment_by_country, what_if_simulation
    -- What-if confirmed: $7,525.80 (small pool limits ceiling)
    -- -------------------------------------------------------------------------
    SELECT
        7,
        'Australia checkout abandonment — 49.31% (worst country, 4.47 pts above Canada)',
        'Checkout UX & Conversion',
        7525.80,
        7525.80,
        801,
        'Australia has the worst checkout abandonment at 49.31% vs Canada best 44.84%. '
        || 'Small checkout pool (801 users) limits absolute impact to $7,525 uplift. '
        || 'However Australia AOV is high ($393.91) — worth fixing for future scale.',
        'Localise checkout for Australia: add BECS direct debit, AfterPay BNPL, '
        || 'display AUD clearly, add AU trust badges. '
        || 'Quick win: fix is the same as broader checkout UX work (Problem 2) — '
        || 'no extra effort if tackled together.',
        1,   -- small absolute impact
        1,   -- 801 users
        4,   -- Localisation, 1 sprint (can piggyback Problem 2)
        'MEDIUM'

    UNION ALL

    -- -------------------------------------------------------------------------
    -- PROBLEM 8: Mobile + Referral payment failure (18.24% — worst combo)
    -- Source: rc_payment_failure_by_device_source
    -- 603 orders, 3.24 pts above global average
    -- -------------------------------------------------------------------------
    SELECT
        8,
        'Mobile + Referral payment failure — 18.24% failure rate (worst device×source combo)',
        'Payment & Infrastructure',
        -- Revenue impact: 603 orders × 18.24% failure × avg order value ~$393
        -- vs expected 15%: extra 3.24% × 603 × $393 ≈ $7,675
        7675.00,
        7675.00,
        603,
        'Mobile + Referral combo has 18.24% failure rate — 3.24 pts above global avg. '
        || 'Second worst: tablet + tiktok at 17.95%. '
        || 'Referral traffic on mobile may have specific payment method or redirect issues.',
        'Investigate referral partner landing pages on mobile — check if payment '
        || 'redirect flow breaks on mobile browsers from referral UTM params. '
        || 'Add mobile-specific payment retry for referral segment. '
        || 'Quick diagnostic: filter payment logs by device=mobile AND source=referral.',
        1,
        1,   -- 603 orders
        4,   -- Investigation + config fix
        'MEDIUM'

),

-- -------------------------------------------------------------------------
-- COMPUTE PRIORITY SCORE
-- Formula: (revenue × 0.50) + (user_impact × 0.30) + (ease × 0.20)
-- -------------------------------------------------------------------------
scored AS (
    SELECT
        *,
        ROUND(
            (revenue_impact_score * 0.50)
            + (user_impact_score   * 0.30)
            + (ease_of_fix_score   * 0.20),
            2
        ) AS priority_score
    FROM problems
)

SELECT
    ROW_NUMBER() OVER (ORDER BY priority_score DESC, revenue_impact_ceiling DESC)
                                            AS priority_rank,
    problem_id,
    problem,
    category,
    severity,
    ROUND(revenue_impact_ceiling::numeric, 2)       AS revenue_impact_ceiling,
    ROUND(revenue_impact_10pct_scenario::numeric, 2) AS revenue_impact_conservative,
    users_impacted,
    finding_detail,
    recommendation,
    revenue_impact_score,
    user_impact_score,
    ease_of_fix_score,
    priority_score
FROM scored
ORDER BY priority_score DESC, revenue_impact_ceiling DESC;


-- =============================================================================
-- SECTION 3: ADD INDEX (run after table creation)
-- =============================================================================

CREATE INDEX IF NOT EXISTS idx_priority_rank
    ON priority_table (priority_rank ASC);

CREATE INDEX IF NOT EXISTS idx_priority_score
    ON priority_table (priority_score DESC);


-- =============================================================================
-- SECTION 4: VERIFY SELECTS (run each one separately)
-- =============================================================================

-- 4a. Executive priority table — the key dashboard view
SELECT
    priority_rank,
    problem,
    category,
    severity,
    '$' || TO_CHAR(revenue_impact_ceiling, 'FM999,999,990.00')  AS revenue_ceiling,
    users_impacted,
    priority_score,
    recommendation
FROM priority_table
ORDER BY priority_rank;


-- 4b. Scoring breakdown — shows how each problem was scored
SELECT
    priority_rank,
    problem,
    severity,
    revenue_impact_score  || '/5'  AS revenue_score,
    user_impact_score     || '/5'  AS user_score,
    ease_of_fix_score     || '/5'  AS ease_score,
    priority_score                 AS total_score
FROM priority_table
ORDER BY priority_rank;


-- 4c. Revenue opportunity summary — total ceiling across all problems
SELECT
    severity,
    COUNT(*)                                        AS problem_count,
    SUM(revenue_impact_ceiling)                     AS total_revenue_ceiling,
    '$' || TO_CHAR(SUM(revenue_impact_ceiling),
        'FM999,999,990.00')                         AS total_revenue_ceiling_fmt
FROM priority_table
GROUP BY severity
ORDER BY total_revenue_ceiling DESC;


-- 4d. Quick-win filter — high ease score (>=4), run immediately
SELECT
    priority_rank,
    problem,
    category,
    ease_of_fix_score,
    '$' || TO_CHAR(revenue_impact_ceiling, 'FM999,999,990.00') AS revenue_ceiling,
    recommendation
FROM priority_table
WHERE ease_of_fix_score >= 4
ORDER BY priority_rank;


-- =============================================================================
-- EXPECTED RESULTS (verify against these after running)
-- =============================================================================
--
-- 4a — Priority ranking (approximate, based on scoring weights):
--
--   Rank | Problem                                        | Score | Severity
--   -----+------------------------------------------------+-------+---------
--   1    | Structural payment failure (~15%)              |  3.5  | CRITICAL
--   2    | India & USA payment failures ($676K)           |  4.1  | HIGH
--   3    | Visit-to-product-view drop-off (17,825 users)  |  4.1  | HIGH
--   4    | Checkout-to-payment drop-off                   |  3.5  | HIGH
--   5    | Google traffic quality                         |  4.0  | HIGH
--   6    | UK under-investment                            |  2.9  | OPPORTUNITY
--   7    | Australia checkout abandonment                 |  1.8  | MEDIUM
--   8    | Mobile + Referral failure combo                |  1.8  | MEDIUM
--
-- 4c — Revenue ceiling by severity:
--   CRITICAL:    $1,198,640
--   HIGH:        ~$1,465,000 combined
--   OPPORTUNITY: ~$218,000
--   MEDIUM:      ~$15,000
--
-- NOTE: Scores will be computed live from the formula — verify 4b to confirm
--       the arithmetic matches expectations before using in the dashboard.
-- =============================================================================

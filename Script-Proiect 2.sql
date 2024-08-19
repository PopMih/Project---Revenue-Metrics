WITH monthly_revenue AS (
    SELECT
        date(strftime('%Y-%m-01', payment_date)) AS payment_month,
        user_id,
        game_name,
        SUM(revenue_amount_usd) AS total_revenue
    FROM games_payments
    GROUP BY payment_month, user_id,game_name
),
revenue_lag_lead_months AS (
    SELECT
        *,
        date(payment_month, '-1 month') AS previous_calendar_month,
        date(payment_month, '+1 month') AS next_calendar_month,
        LAG(total_revenue) OVER (PARTITION BY user_id ORDER BY payment_month) AS previous_paid_month_revenue,
        LAG(payment_month) OVER (PARTITION BY user_id ORDER BY payment_month) AS previous_paid_month,
        LEAD(payment_month) OVER (PARTITION BY user_id ORDER BY payment_month) AS next_paid_month
    FROM monthly_revenue
),
revenue_metrics AS (
    SELECT
        payment_month,
        user_id,
        game_name,
        total_revenue,
        CASE 
            WHEN previous_paid_month IS NULL THEN total_revenue
        END AS new_mrr,
        CASE 
            WHEN previous_paid_month = previous_calendar_month AND total_revenue > previous_paid_month_revenue THEN total_revenue - previous_paid_month_revenue
        END AS expansion_revenue,
        CASE 
            WHEN previous_paid_month = previous_calendar_month AND total_revenue < previous_paid_month_revenue THEN total_revenue - previous_paid_month_revenue
        END AS contraction_revenue,
        CASE 
            WHEN previous_paid_month != previous_calendar_month AND previous_paid_month IS NOT NULL THEN total_revenue
        END AS back_from_churn_revenue,
        CASE 
            WHEN next_paid_month IS NULL OR next_paid_month != next_calendar_month THEN total_revenue
        END AS churned_revenue,
        CASE 
            WHEN next_paid_month IS NULL OR next_paid_month != next_calendar_month THEN next_calendar_month
        END AS churn_month
    FROM revenue_lag_lead_months
),
arppu_metrics AS (
    SELECT
        payment_month,
        game_name,
        SUM(total_revenue) AS total_revenue,
        COUNT(DISTINCT user_id) AS paying_users,
        SUM(total_revenue) / COUNT(DISTINCT user_id) AS arppu,
        COUNT(DISTINCT CASE WHEN churned_revenue IS NOT NULL THEN user_id END) AS churned_users,
        SUM(CASE WHEN churned_revenue IS NOT NULL THEN total_revenue ELSE 0 END) AS churned_mrr,
        COUNT(DISTINCT CASE WHEN churned_revenue IS NOT NULL THEN user_id END) * 1.0 / COUNT(DISTINCT user_id) AS churned_rate,
        COUNT(DISTINCT user_id) AS paid_users,
        COUNT(DISTINCT CASE WHEN new_mrr IS NOT NULL THEN user_id END) AS new_paid_users
    FROM revenue_metrics
    GROUP BY payment_month, game_name
)
SELECT
    rm.*,
    gpu.language,
    gpu.has_older_device_model,
    gpu.age,
    am.arppu,
    am.paid_users
FROM revenue_metrics rm
LEFT JOIN games_paid_users gpu USING (user_id)
LEFT JOIN arppu_metrics am ON rm.payment_month = am.payment_month AND rm.game_name = am.game_name;
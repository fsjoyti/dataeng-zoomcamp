{{
    config(
        materialized='view'
    )
}}

WITH QuarterlyRevenue AS (
    SELECT
        pickup_year,
        pickup_quarter,
        service_type,
        SUM(total_amount) AS total_revenue
    FROM
        {{ ref('fact_trips') }}
    WHERE pickup_year in (2019, 2020)
    GROUP BY
        pickup_year,
        pickup_quarter,
        service_type
)
SELECT
    qr_current.pickup_year,
    qr_current.pickup_quarter,
    qr_current.service_type,
    qr_current.total_revenue AS current_quarter_revenue,
    qr_previous.total_revenue AS previous_quarter_revenue,
    (qr_current.total_revenue - qr_previous.total_revenue) AS revenue_growth,
    (
        (qr_current.total_revenue - qr_previous.total_revenue) * 100.0 / qr_previous.total_revenue
    ) AS yoy_growth_percentage
FROM
    QuarterlyRevenue qr_current
JOIN
    QuarterlyRevenue qr_previous
        ON qr_current.pickup_quarter = qr_previous.pickup_quarter
        AND qr_current.pickup_year = qr_previous.pickup_year + 1
        AND qr_current.service_type = qr_previous.service_type
ORDER BY
    qr_current.pickup_year,
    qr_current.pickup_quarter,
    qr_current.service_type
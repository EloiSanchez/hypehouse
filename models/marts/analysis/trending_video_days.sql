WITH trending_day_table AS (
    SELECT * FROM {{ ref('tbl_trending_day') }}
)
SELECT 
    video_id,
    COUNT(video_id) AS days_trending,
    MIN(trending_date) AS start_trending,
    start_trending + days_trending - 1 AS end_trending,
    CASE
        WHEN end_trending = (SELECT MAX(trending_date) FROM {{ ref('tbl_trending_day') }}) THEN TRUE
        ELSE FALSE
    END AS currently_trending
FROM trending_day_table
GROUP BY video_id
ORDER BY days_trending DESC, currently_trending DESC, end_trending DESC
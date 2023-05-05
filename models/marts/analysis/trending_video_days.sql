WITH trending_day_table AS (
    SELECT * FROM {{ ref('wh_trending_day') }}
)
SELECT 
    video_id,
    COUNT(video_id) AS days_trending,
    MIN(trending_date) AS first_trending,
    MAX(trending_date) AS last_trending,
    CASE
        WHEN last_trending >= current_date() - 1 THEN TRUE
        ELSE FALSE
    END AS currently_trending
FROM trending_day_table
GROUP BY video_id
ORDER BY days_trending DESC, currently_trending DESC, last_trending DESC
-- comment
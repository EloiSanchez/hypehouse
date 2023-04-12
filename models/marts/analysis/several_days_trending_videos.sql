WITH trending_day_table AS (
    SELECT * FROM {{ ref('tbl_trending_day') }}
)
SELECT
    video_id,
    COUNT(*) as days_trending
FROM trending_day_table
GROUP BY video_id
HAVING days_trending > 1
ORDER BY days_trending DESC

SELECT
    video_id,
    COUNT(*) as days_trending
FROM tbl_trending_day
GROUP BY video_id
HAVING days_trending > 1
ORDER BY days_trending DESC

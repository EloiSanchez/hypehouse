SELECT 
    video_id,
    COUNT(video_id) AS days_trending,
    MIN(trending_date) AS start_trending,
    start_trending + days_trending - 1 AS end_trending,
    CASE
        WHEN end_trending = (SELECT MAX(trending_date) FROM tbl_trending_day) THEN TRUE
        ELSE FALSE
    END AS currently_trending
FROM TBL_TRENDING_DAY
GROUP BY video_id
ORDER BY days_trending DESC, currently_trending DESC, end_trending DESC
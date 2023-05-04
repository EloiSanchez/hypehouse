WITH video_tags AS (
    SELECT * FROM {{ ref('wh_tags') }}
),
trending_day AS (
    SELECT * FROM {{ ref('wh_trending_day') }}
)
SELECT
    YEAR(trending_day.trending_date) AS year,
    WEEK(trending_day.trending_date) AS week,
    video_tags.tag_name AS tag_name,
    COUNT(video_tags.tag_name) AS num_count
FROM video_tags
LEFT JOIN trending_day
    ON video_tags.video_id = trending_day.video_id
GROUP BY year, week, tag_name
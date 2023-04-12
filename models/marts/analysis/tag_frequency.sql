WITH video_tags AS (
    SELECT * FROM {{ ref('tbl_tags') }}
)
SELECT
    tag_name,
    COUNT(tag_name) AS num_count
FROM video_tags
GROUP BY tag_name
HAVING num_count > 1
ORDER BY num_count DESC
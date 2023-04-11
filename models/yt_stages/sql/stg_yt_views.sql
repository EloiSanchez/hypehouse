WITH unique_views AS (
    SELECT DISTINCT src FROM {{ source('raw_yt', 'raw_yt_views') }}
)
SELECT
    src:video_id::varchar as video_id,
    src:view_count::int as view_count
FROM unique_views
WHERE 1 = 1
    AND video_id != 'video_id'
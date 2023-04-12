WITH unique_likes AS (
    SELECT DISTINCT * FROM {{ source('raw_yt', 'raw_yt_likes') }}
)
SELECT
    src:video_id::varchar as video_id,
    src:like_count::int as like_count,
    loaded_date
FROM unique_likes
WHERE 1 = 1
    AND video_id != 'video_id'

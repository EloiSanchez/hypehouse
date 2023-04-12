WITH unique_comments AS (
    SELECT DISTINCT * FROM {{ source('raw_yt', 'raw_yt_comments') }}
)
SELECT
    src:video_id::varchar as video_id,
    src:comment_count::varchar as comment_count,
    loaded_date
FROM unique_comments
WHERE 1 = 1
    AND video_id != 'video_id'

SELECT DISTINCT
    TRIM(channel_id) AS channel_id,
    TRIM(channel_title) AS channel_title
FROM {{ ref('ranked_yt_videos') }}
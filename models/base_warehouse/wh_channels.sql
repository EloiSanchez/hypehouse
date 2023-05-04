SELECT DISTINCT
    TRIM(channel_id) AS channel_id,
    TRIM(channel_title) AS channel_title
FROM {{ ref('stg_yt_videos') }}
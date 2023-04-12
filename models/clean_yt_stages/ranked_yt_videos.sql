SELECT 
    *, 
    ROW_NUMBER() OVER (PARTITION BY video_id ORDER BY trending_date, comment_count) as trending_day
FROM {{ ref('stg_yt_videos') }}
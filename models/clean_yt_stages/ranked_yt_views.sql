SELECT 
    *, 
    ROW_NUMBER() OVER (PARTITION BY video_id ORDER BY loaded_date, view_count) as trending_day
FROM {{ ref('stg_yt_views') }}
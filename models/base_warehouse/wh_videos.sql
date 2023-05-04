WITH videos AS (
    SELECT * FROM {{ ref('stg_yt_videos') }}
),
category_dict AS (
    SELECT * FROM {{ ref('us_yt_categories') }}
)
SELECT DISTINCT
    videos.video_id,
    MAX_BY(videos.channel_id, trending_date) as channel_id,
    MAX_BY(category_dict.category, trending_date) as category,
    MAX_BY(videos.published_at, trending_date) as published_at,
    MAX_BY(videos.comments_disabled, trending_date) as comments_disabled,
    MAX_BY(videos.ratings_disabled, trending_date) as ratings_disabled,
    MAX_BY(videos.thumbnail_link, trending_date) as thumbnail_link
FROM videos
    LEFT JOIN category_dict
        ON videos.category_id = category_dict.category_id
GROUP BY videos.video_id
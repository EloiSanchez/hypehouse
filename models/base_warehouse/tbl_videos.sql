WITH videos AS (
    SELECT * FROM {{ ref('stg_yt_videos') }}
),
comments AS (
    SELECT * FROM {{ ref('stg_yt_comments') }}
),
category_dict AS (
    SELECT * FROM {{ ref('us_yt_categories') }}
)
SELECT DISTINCT
    videos.video_id,
    videos.channel_id,
    category_dict.category,
    videos.published_at,
    videos.comments_disabled,
    videos.ratings_disabled,
    comments.comment_count as thumbnail_link
FROM videos
    FULL OUTER JOIN comments
        ON videos.video_id = comments.video_id
    LEFT JOIN category_dict
        ON videos.category_id = category_dict.category_id

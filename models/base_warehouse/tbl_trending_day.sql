WITH videos AS (
    SELECT * FROM {{ ref('ranked_yt_videos') }}
),
yt_views AS (
    SELECT * FROM {{ ref('ranked_yt_views') }}
),
likes as (
    SELECT * FROM {{ ref('ranked_yt_likes') }}
)
SELECT
    videos.video_id,
    videos.trending_date,
    videos.title,
    videos.comment_count,
    yt_views.view_count,
    likes.like_count
FROM videos
    FULL OUTER JOIN yt_views
        ON videos.video_id = yt_views.video_id AND videos.loaded_date = yt_views.loaded_date AND videos.trending_day = yt_views.trending_day
    FULL OUTER JOIN likes
        ON videos.video_id = likes.video_id AND videos.loaded_date = likes.loaded_date AND videos.trending_day = likes.trending_day
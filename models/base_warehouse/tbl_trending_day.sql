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
    -- count(*) as c
FROM videos
    LEFT JOIN yt_views
        ON videos.video_id = yt_views.video_id AND videos.trending_day = yt_views.trending_day
    LEFT JOIN likes
        ON videos.video_id = likes.video_id AND videos.trending_day = likes.trending_day
-- group by videos.video_id, videos.trending_date
-- having c>1
-- where videos.video_id =  'cS_lzYjLgYA'
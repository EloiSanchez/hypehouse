WITH videos AS (
    SELECT * FROM {{ ref('stg_yt_videos') }}
),
yt_views AS (
    SELECT * FROM {{ ref('stg_yt_views') }}
),
likes as (
    SELECT * FROM {{ ref('stg_yt_likes') }}
),
comments as (
    SELECT * FROM {{ ref('stg_yt_comments') }}
)
SELECT DISTINCT
    videos.video_id,
    videos.trending_date,
    videos.title,
    comments.comment_count,
    yt_views.view_count,
    likes.like_count
    -- count(*) as c
FROM videos
    LEFT JOIN yt_views
        ON videos.video_id = yt_views.video_id AND videos.trending_date = yt_views.trending_date
    LEFT JOIN likes
        ON videos.video_id = likes.video_id AND videos.trending_date = likes.trending_date
    LEFT JOIN comments
        ON videos.video_id = comments.video_id AND videos.trending_date = comments.trending_date
order by videos.video_id, videos.trending_date
-- group by videos.video_id, videos.trending_date
-- having c>1
-- where videos.video_id =  'cS_lzYjLgYA'

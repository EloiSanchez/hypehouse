with channels as (
    select * from {{ ref('wh_channels') }}
),

trending_day as (
    select * from {{ ref('wh_trending_day') }}
),

videos as (
    select * from {{ ref('wh_videos') }}
)

select
    channels.channel_title,
    YEAR(trending_day.trending_date) as year,
    MONTH(trending_day.trending_date) as month,
    COUNT(DISTINCT trending_day.video_id) as trending_videos
from trending_day
inner join videos
    on videos.video_id = trending_day.video_id
inner join channels
    on videos.channel_id = channels.channel_id
group by channels.channel_title, year, month
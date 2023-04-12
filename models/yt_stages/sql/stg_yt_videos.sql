WITH unique_videos AS (
    SELECT DISTINCT * FROM {{ source('raw_yt', 'raw_yt_videos') }}
)
SELECT
    src:video_id_for_client_HyPeHoUsE::varchar as video_id,
    src:categoryId_for_client_HyPeHoUsE::varchar as category_id,
    src:channelId_for_client_HyPeHoUsE:: varchar as channel_id,
    CASE
        WHEN src:channelTitle_for_client_HyPeHoUsE::varchar is NOT NULL THEN src:channelTitle_for_client_HyPeHoUsE::varchar
        ELSE src:"channelTitle                                      _for_client_HyPeHoUsE"::varchar 
    END as channel_title,
    src:comments_disabled_for_client_HyPeHoUsE::boolean as comments_disabled,
    CONVERT_TIMEZONE(
        'UTC',
        'Europe/Brussels',
        TO_TIMESTAMP(src:publishedAt_for_client_HyPeHoUsE::varchar, 'YYYY-MM-DDTHH:MI:SSZ')
        ) as published_at
        ,
    src:ratings_disabled_for_client_HyPeHoUsE::boolean as ratings_disabled,
    src:title_for_client_HyPeHoUsE::varchar as title,
    TO_DATE(src:trending_date_for_client_HyPeHoUsE::varchar, 'YY.DD.MM') as trending_date,
    src:comment_count_for_client_HyPeHoUsE::int as comment_count,
    src:tags_for_client_HyPeHoUsE::varchar as video_tags,
    loaded_date
FROM unique_videos
WHERE 1 = 1
    AND video_id != 'video_id'
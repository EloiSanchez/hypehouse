WITH unique_videos AS (
    SELECT DISTINCT * FROM {{ source('raw_yt', 'raw_yt_videos') }}
),
parsed_videos AS (
    SELECT
        src:video_id_for_client_HyPeHoUsE::varchar as video_id,
        src:categoryId_for_client_HyPeHoUsE::varchar as category_id,
        src:channelId_for_client_HyPeHoUsE:: varchar as channel_id,
        CASE
            WHEN src:channelTitle_for_client_HyPeHoUsE::varchar is NOT NULL THEN src:channelTitle_for_client_HyPeHoUsE::varchar
            ELSE src:"channelTitle                                      _for_client_HyPeHoUsE"::varchar 
        END as channel_title,
        src:comments_disabled_for_client_HyPeHoUsE::boolean as comments_disabled,
        TO_TIMESTAMP_TZ(CONVERT_TIMEZONE(
            'UTC',
            'Europe/Brussels',
            TO_TIMESTAMP(src:publishedAt_for_client_HyPeHoUsE::varchar, 'YYYY-MM-DDTHH:MI:SSZ')
            )) as published_at
            ,
        src:ratings_disabled_for_client_HyPeHoUsE::boolean as ratings_disabled,
        MAX(src:title_for_client_HyPeHoUsE::varchar) as title,
        TRY_TO_DATE(src:trending_date_for_client_HyPeHoUsE::varchar, 'YY.DD.MM') as trending_date,
        src:tags_for_client_HyPeHoUsE::varchar as video_tags,
        MAX(src:comment_count_for_client_HyPeHoUsE::int) as comment_count,
        MAX(loaded_date)
    FROM unique_videos
    WHERE 1 = 1
        AND video_id != 'video_id'
    GROUP BY video_id, category_id, channel_id, channel_title, published_at,
        comments_disabled, ratings_disabled, trending_date,
        video_tags
),
parsed_comments AS (
    SELECT DISTINCT
        src:video_id_for_client_HyPeHoUsE::varchar as video_id,
        TRY_TO_DATE(src:trending_date_for_client_HyPeHoUsE::varchar, 'YY.DD.MM') as trending_date,
        MAX(src:comment_count_for_client_HyPeHoUsE::int) as comment_count
    FROM unique_videos
    WHERE 1 = 1
        AND video_id != 'video_id'
    GROUP BY video_id, trending_date
)
SELECT * FROM parsed_videos
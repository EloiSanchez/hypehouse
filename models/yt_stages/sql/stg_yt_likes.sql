WITH unique_likes AS (
    SELECT DISTINCT 
        parsed.value AS src,
        loaded_date
    FROM {{ source('raw_yt', 'raw_yt_likes_v2') }} AS tab, table(flatten(tab.src:data)) AS parsed
)
SELECT
    src:video_id_for_client_HyPeHoUsE::varchar as video_id,
    src:likes_for_client_HyPeHoUsE::int as like_count,
    TRY_TO_DATE(src:trending_date_for_client_HyPeHoUsE::varchar, 'YY.DD.MM') as trending_date,
    loaded_date
FROM unique_likes
WHERE 1 = 1
WITH unique_comments AS (
    SELECT DISTINCT 
        parsed.value AS src,
        loaded_date
    FROM {{ source('raw_yt', 'raw_yt_comments_v2') }} AS tab, table(flatten(tab.src:data)) AS parsed
)
SELECT
    src:video_id_for_client_HyPeHoUsE::varchar as video_id,
    src:comment_count_for_client_HyPeHoUsE::varchar as comment_count,
    TRY_TO_DATE(src:trending_date_for_client_HyPeHoUsE::varchar, 'YY.DD.MM') as trending_date,
    loaded_date
FROM unique_comments
WHERE 1 = 1
    AND video_id != 'video_id'

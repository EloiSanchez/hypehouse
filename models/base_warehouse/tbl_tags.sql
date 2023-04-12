SELECT DISTINCT
    video_id,
    trim(tag_list.value, ' ') AS tag_name
FROM 
    {{ ref('stg_yt_videos') }}, 
    TABLE(FLATTEN(SPLIT(TRIM(video_tags, '[]'), '|'))) tag_list
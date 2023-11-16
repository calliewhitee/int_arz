with article_master_source as (
    select * from {{ source('public', 'article_master')}}
), renamed_recast as (
    select
        material as article_id,
        department
    from article_master_source
)
select * from renamed_recast

with article_master_source as (
    select * from {{ source('public', 'article_master')}}
), renamed_recast as (
    select
        material::varchar as articles_id,
        case
            when lower(department) = 't-shirt'
                then 'tshirt'
            when lower(department) = 'face masks'
                then 'headwear'
            else lower(department)
        end as department
    from article_master_source
)
select * from renamed_recast

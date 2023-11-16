with transactions_source as (
    select * from {{ ref('stg_public__transactions') }}
    where action_code = 'PKOCLOSE'
), articles_source as (
    select * from {{ ref('stg_public__articles') }}
), joined_tables as (
    select *
    from transactions_source
        left join articles_source
            on transactions_source.article_id = stg_public__articles.article_id
)
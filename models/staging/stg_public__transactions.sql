with transactions_source as (
    select * from {{ source('public', 'transactions') }}
), renamed_recast as (
    select
        --ID fields starting with primary key, then forgein keys 
        transaction_id,
        warehouse_id, 
        substring(user_id, 5, length(user_id)) as user_id,
        article as article_id,
        ship_line_id,

        --Code Identifiers/Categorizations
        order_line_item,
        order_number,
        order_type,
        order_category,
        order_type_category,
        order_channel,
        inventory_detail_number,
        action_code,
        device_code,
        from_area_code,
        to_area_code,
        from_storage_location,
        to_storage_location, 
        gift_flag,

        --Date/Time Fields
        transaction_timestamp as transaction_at, 
        '2023-11-02' as transaction_date,
        --the data import had issues reading the raw data file and set the date to Nov 21, 2023
        -- manually overwritting for the purposes here because the date is consistent but wouldn't do this in production
        
        --Numeric Fields 
        quantity
    from transactions_source
)

select * from renamed_recast

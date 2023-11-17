with transactions_source as (
    select * from {{ ref('stg_public__transactions') }}
    where action_code = 'PKOCLOSE'
), articles_source as (
    select * from {{ ref('stg_public__articles') }}
), packing_time_standards_source as (
    select * from {{ ref('stg_seeds__packing_time_standards') }}
), joined_tables as (
    select
        transactions_source.*,
        articles_source.department,
        packing_time_standards_source.packing_standard_seconds
    from transactions_source
        left join articles_source
            on transactions_source.articles_id = articles_source.articles_id
        left join packing_time_standards_source
            on articles_source.department = packing_time_standards_source.department
), group_user_order as (
    select
        user_id,
        transaction_at as order_start_at,
        lag(transaction_at, 1) over (partition by user_id order by transaction_at desc) as order_end_at,
        string_agg(distinct department, ', ') as item_departments,
        sum(quantity::int) as number_of_items,
        sum(packing_standard_seconds)
            + {{ basic_move_standard_seconds() }}
            + {{ basic_proc_standard_seconds() }}
            + {{ basic_scan_standard_seconds() }}
            + {{ basic_wrap_standard_seconds() }}
            + {{ basic_pack_standard_seconds() }} as total_packing_standard_seconds
        
    from joined_tables
    group by 1, 2
), calculate_duration as (
    select *,
        (date_part('hour', order_end_at::time
            - order_start_at::time) * 60 +
            date_part('minute', order_end_at::time
                - order_start_at::time)) * 60 +
            date_part('second', order_end_at::time
                - order_start_at::time) as order_pack_duration_seconds
    from group_user_order
), calculate_duration_difference as (
    select
        *,
        total_packing_standard_seconds - order_pack_duration_seconds as difference_to_standard
    from calculate_duration
)
select * from calculate_duration_difference

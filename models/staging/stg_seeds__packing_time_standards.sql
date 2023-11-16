with packing_time_standards_source as (
    select * from {{ ref('packing_time_standards') }}
), renamed_recast as (
    select
        basicmove_standard,
        basicproc_standard,
        basicscan_standard,
        basicwrap_standard,
        basicpack_standard,
        belt_standard,
        blouse_standard,
        coat_standard,
        dress_standard,
        footwear_standard,
        headwear_standard,
        hosiery_standard,
        jacket_standard,
        neckwear_standard,
        pant_standard,
        skirt_standard,
        sweater_standard,
        tshirt_standard,
        underwear_standard,
        bag_standard,
        giftitem_standard,
        handwear_standard,
        smallgood_standard,
        jewelry_standard,
        swimwear_standard
    from packing_time_standards_source
)
select * from renamed_recast

with base as (SELECT
    orders.order_id,
    orders.status,
    orders.is_cancelled,
    orders.customer_id,
    orders.manufacturer_id,
    orders.customer_price,
    orders.manufacturer_price,
    orders.shipping_price,
    orders.markup,
    orders.account_manager_country,
    orders.created_at,
    orders.in_production_at
FROM
    {{ source('fractory', 'orders') }} AS orders),

order_parts_count as (SELECT
    parts_extended.order_id,
    count(DISTINCT parts_extended.order_part_id) AS order_parts_count
FROM
   {{ ref('maris_parts_ext') }} AS parts_extended
    GROUP BY parts_extended.order_id),

cnc_parts_count as (SELECT
                parts_extended.order_id,
    sum(
        CASE
            WHEN
                parts_extended.selected_process_type = 'cnc_machining'
                THEN 1
            ELSE 0
        END
    ) AS cnc_parts_count
FROM
   {{ ref('maris_parts_ext') }} AS parts_extended
    GROUP BY parts_extended.order_id),

laser_parts_count as (SELECT
                      parts_extended.order_id,
    sum(
        CASE
            WHEN
                parts_extended.selected_process_type = 'laser_cutting'
                THEN 1
            ELSE 0
        END
    ) AS laser_parts_count
                      FROM
   {{ ref('maris_parts_ext') }} AS parts_extended
    GROUP BY parts_extended.order_id),

bending_parts_count as (SELECT
                      parts_extended.order_id,
    sum(parts_extended.has_bending) AS bending_parts_count
FROM
    {{ ref('maris_parts_ext') }} AS parts_extended
GROUP BY parts_extended.order_id),

bends_count as (SELECT
                      parts_extended.order_id,
    sum(bends_count) AS total_bends_count
FROM
    {{ ref('maris_parts_ext') }} AS parts_extended
GROUP BY parts_extended.order_id),

surface_coating_parts_count as (SELECT
                      parts_extended.order_id,
    countif(has_surface_coating) AS surface_coating_parts_count
FROM
    {{ ref('maris_parts_ext') }} AS parts_extended
GROUP BY parts_extended.order_id),

insert_operations_parts_count as (SELECT
                      parts_extended.order_id,
    countif(has_insert_operations) AS insert_operations_parts_count
FROM
    {{ ref('maris_parts_ext') }} AS parts_extended
GROUP BY parts_extended.order_id),

unique_ral_codes as (SELECT
                      parts_extended.order_id,
    string_agg(DISTINCT parts_extended.ral_code) AS unique_ral_codes
FROM
    {{ ref('maris_parts_ext') }} AS parts_extended
GROUP BY parts_extended.order_id),

unique_ral_finishes as (SELECT
                      parts_extended.order_id,
    string_agg(DISTINCT parts_extended.ral_finish) AS unique_ral_finishes
FROM
    {{ ref('maris_parts_ext') }} AS parts_extended
GROUP BY parts_extended.order_id),

unique_surface_finishes as (SELECT
                      parts_extended.order_id,
    string_agg(DISTINCT parts_extended.surface_finish) AS surface_finishes
FROM
    {{ ref('maris_parts_ext') }} AS parts_extended
GROUP BY parts_extended.order_id),

unique_secondary_finishes as (SELECT
                      parts_extended.order_id,
    string_agg(DISTINCT parts_extended.secondary_finish) AS secondary_finishes
FROM
    {{ ref('maris_parts_ext') }} AS parts_extended
GROUP BY parts_extended.order_id),

final as (SELECT
    base.order_id,
    base.status,
    base.is_cancelled,
    base.customer_id,
    base.manufacturer_id,
    base.customer_price,
    base.manufacturer_price,
    base.shipping_price,
    base.markup,
    base.account_manager_country,
    base.created_at,
    base.in_production_at,
order_parts_count.order_parts_count,
cnc_parts_count.cnc_parts_count,
laser_parts_count.laser_parts_count,
bending_parts_count.bending_parts_count,
bends_count.total_bends_count,
surface_coating_parts_count.surface_coating_parts_count,
insert_operations_parts_count.insert_operations_parts_count,
unique_ral_codes.unique_ral_codes,
unique_ral_finishes.unique_ral_finishes,
unique_surface_finishes.surface_finishes,
unique_secondary_finishes.secondary_finishes
from base
left join order_parts_count on base.order_id = order_parts_count.order_id
left join cnc_parts_count on base.order_id = cnc_parts_count.order_id
left join laser_parts_count on base.order_id = laser_parts_count.order_id
left join bending_parts_count on base.order_id = bending_parts_count.order_id
left join bends_count on base.order_id = bends_count.order_id
left join surface_coating_parts_count on base.order_id = surface_coating_parts_count.order_id
left join insert_operations_parts_count on base.order_id = insert_operations_parts_count.order_id
left join unique_ral_codes on base.order_id = unique_ral_codes.order_id
left join unique_ral_finishes on base.order_id = unique_ral_finishes.order_id
left join unique_surface_finishes on base.order_id = unique_surface_finishes.order_id
left join unique_secondary_finishes on base.order_id = unique_secondary_finishes.order_id
)

select * from final
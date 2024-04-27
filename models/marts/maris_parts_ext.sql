SELECT
    parts.order_part_id,
    parts.order_id,
    parts.selected_process_type,
    parts.material_name,
    parts.material_type,
    parts.weight_g,
    parts.quantity,
    parts.manufacturer_price_eur,
    parts.has_bending,
    parts.has_surface_coating,
    parts.has_insert_operations,
    parts.bends_count,
    parts.created_at,
    replace(json_extract(surface_finish.process_config, "$.value"), '"', "")
        AS surface_finish,
    replace(json_extract(secondary_finish.process_config, "$.value"), '"', "")
        AS secondary_finish,
    replace(json_extract(finish_ral.process_config, "$.ralCode"), '"', "")
        AS ral_code,
    replace(json_extract(finish_ral.process_config, "$.ralFinish"), '"', "")
        AS ral_finish
FROM
    {{ source("fractory", "parts") }} AS parts
LEFT JOIN
    {{ source("fractory", "parts_surface_finish_config") }}
        AS surface_finish
    ON
        parts.order_part_id = surface_finish.order_part_id
        AND surface_finish.process_name = "SURFACE_FINISH"
LEFT JOIN
    {{ source("fractory", "parts_surface_finish_config") }}
        AS secondary_finish
    ON
        parts.order_part_id = secondary_finish.order_part_id
        AND secondary_finish.process_name = "SECONDARY_SURFACE_FINISH"
LEFT JOIN
    {{ source("fractory", "parts_surface_finish_config") }}
        AS finish_ral
    ON
        parts.order_part_id = finish_ral.order_part_id
        AND finish_ral.process_name = "SECONDARY_SURFACE_FINISH_RAL"

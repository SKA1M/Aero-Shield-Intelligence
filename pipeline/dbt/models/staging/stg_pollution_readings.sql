{{
    config(
        materialized='view',
        tags=['staging', 'pollution']
    )
}}

-- Stage raw pollution readings from the curated S3 zone loaded into Redshift.
-- This view standardises column names, casts types, and applies basic
-- quality filters (non-null location, valid PM2.5 range).

WITH source AS (

    SELECT *
    FROM {{ source('curated', 'worker_exposure_raw') }}
    WHERE run_date = '{{ var("run_date") }}'

),

cleaned AS (

    SELECT
        location_name,
        latitude::FLOAT                         AS latitude,
        longitude::FLOAT                        AS longitude,
        pm25::FLOAT                             AS pm25_ug_m3,
        NULLIF(pm10, 0)::FLOAT                  AS pm10_ug_m3,
        NULLIF(no2, 0)::FLOAT                   AS no2_ug_m3,
        NULLIF(o3, 0)::FLOAT                    AS o3_ug_m3,
        wind_speed_m_s::FLOAT                   AS wind_speed_m_s,
        pressure_hpa::FLOAT                     AS pressure_hpa,
        temperature_c::FLOAT                    AS temperature_c,
        humidity_percent::INT                   AS humidity_percent,
        mixing_height_m::FLOAT                  AS mixing_height_m,
        ventilation_index::FLOAT                AS ventilation_index,
        road_type::VARCHAR                      AS road_type,
        urban_class::VARCHAR                    AS urban_class,
        street_aspect_ratio::FLOAT              AS street_aspect_ratio,
        pop_density_km2::INT                    AS pop_density_km2,
        last_updated_local::TIMESTAMP           AS reading_timestamp,
        run_date::DATE                          AS pipeline_run_date
    FROM source
    WHERE location_name IS NOT NULL
      AND pm25 BETWEEN 0 AND 1000
      AND latitude BETWEEN -90 AND 90
      AND longitude BETWEEN -180 AND 180

)

SELECT * FROM cleaned

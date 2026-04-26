{{
    config(
        materialized='table',
        tags=['marts', 'production'],
        dist='location_name',
        sort=['pipeline_run_date', 'location_name']
    )
}}

-- Final analytics table: computes all biomedical impact metrics per site per day.
-- This is the table powering the Streamlit dashboard and executive reporting.
--
-- Formulas mirror those in src/models.py, expressed in pure SQL so that
-- Redshift (not Python) does the heavy aggregation work.
--
-- Constants (from dbt_project.yml vars):
--   cigarette_pm25_factor   = 22.0 ug/m3 per cigarette-equivalent (Berkeley Earth)
--   breathing_rate_m3_hr    = 1.5 m3/hr (Active Worker Persona, 25 L/min)
--   filter_efficiency       = 0.325 (E11 Pleated Filter, realistic)

WITH staged AS (

    SELECT *
    FROM {{ ref('stg_pollution_readings') }}

),

penalties AS (

    SELECT
        *,

        -- Stagnation penalty: 1.5 if VI < 1000, else 1.0
        CASE WHEN ventilation_index < 1000 THEN 1.5 ELSE 1.0 END
            AS stagnation_penalty,

        -- Source penalty: 1.3 for arterial roads (FRC1/FRC2), else 1.1
        CASE WHEN road_type IN ('FRC1', 'FRC2') THEN 1.3 ELSE 1.1 END
            AS source_penalty

    FROM staged

),

bio_impact AS (

    SELECT
        *,

        -- Inhaled dose per hour (ug/hr)
        ROUND(
            pm25_ug_m3 * {{ var('breathing_rate_m3_hr') }}
            * stagnation_penalty * source_penalty,
            2
        ) AS inhaled_dose_ug_hr,

        -- Cigarette equivalence for 8-hour shift
        ROUND(
            (pm25_ug_m3 * {{ var('breathing_rate_m3_hr') }}
             * stagnation_penalty * source_penalty * 8.0)
            / ({{ var('cigarette_pm25_factor') }} * 24.0),
            2
        ) AS shift_cigarette_equiv,

        -- OWPEI composite risk score
        ROUND(
            pm25_ug_m3 * stagnation_penalty * source_penalty
            * (street_aspect_ratio / 2.0),
            1
        ) AS owpei

    FROM penalties

),

final AS (

    SELECT
        -- Identity
        location_name,
        latitude,
        longitude,
        pipeline_run_date,
        reading_timestamp,

        -- Raw environmental readings
        pm25_ug_m3,
        pm10_ug_m3,
        no2_ug_m3,
        o3_ug_m3,
        temperature_c,
        humidity_percent,
        wind_speed_m_s,
        pressure_hpa,

        -- Derived atmospheric metrics
        mixing_height_m,
        ventilation_index,

        -- Urban context
        road_type,
        urban_class,
        street_aspect_ratio,
        pop_density_km2,

        -- Penalty multipliers (for traceability)
        stagnation_penalty,
        source_penalty,

        -- Biomedical impact
        inhaled_dose_ug_hr,
        shift_cigarette_equiv,

        -- Protected exposure (residual after Aero Shield filter)
        ROUND(
            shift_cigarette_equiv * (1.0 - {{ var('filter_efficiency') }}),
            2
        ) AS protected_cigarette_equiv,

        ROUND(
            shift_cigarette_equiv * {{ var('filter_efficiency') }},
            2
        ) AS cigs_prevented_per_shift,

        -- Composite scores
        owpei,
        ROUND(
            (owpei * pop_density_km2) / 1000.0,
            1
        ) AS aero_shield_tam_index,

        -- Classifications
        CASE
            WHEN pm25_ug_m3 > 15 THEN 'TOXIC'
            ELSE 'SAFE'
        END AS toxic_status,

        CASE
            WHEN shift_cigarette_equiv < 1.0  THEN 'Low Risk'
            WHEN shift_cigarette_equiv < 3.0  THEN 'Moderate Risk'
            WHEN shift_cigarette_equiv < 6.0  THEN 'High Risk'
            WHEN shift_cigarette_equiv < 10.0 THEN 'Extreme Risk'
            ELSE 'Critical Risk'
        END AS risk_tier

    FROM bio_impact

)

SELECT * FROM final
ORDER BY aero_shield_tam_index DESC

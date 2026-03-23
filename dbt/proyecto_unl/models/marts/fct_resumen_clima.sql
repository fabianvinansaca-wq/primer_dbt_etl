{{ config(materialized='table') }}

SELECT
    nombre_ciudad,
    temperatura,
    {{ celsius_a_fahrenheit('temperatura') }} AS temperatura_fahrenheit,
    clima
FROM {{ ref('stg_clima') }}

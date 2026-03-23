{{ config(materialized='table') }}

SELECT
    c.nombre_ciudad,
    c.temperatura,
    {{ celsius_a_fahrenheit('c.temperatura') }} AS temperatura_fahrenheit,
    c.clima,
    a.indice_ica,
    a.particulas_pm25,
    {{ clasificacion_aire('a.indice_ica') }} AS categoria_calidad_aire
FROM {{ ref('stg_clima') }} c
INNER JOIN {{ ref('stg_calidad_aire') }} a
    ON c.nombre_ciudad = a.nombre_ciudad

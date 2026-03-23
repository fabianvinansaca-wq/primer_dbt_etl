-- Modelo staging para datos climáticos
SELECT
    ciudad AS nombre_ciudad,             -- Estandarización del nombre de columna
    temperatura,
    clima
FROM {{ source('raw_data', 'datos_clima') }}

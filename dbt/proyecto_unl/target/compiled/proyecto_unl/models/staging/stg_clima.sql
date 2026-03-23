-- Modelo staging para datos climáticos
SELECT
    ciudad AS nombre_ciudad,             -- Estandarización del nombre de columna
    temperatura,
    clima
FROM "db_warehouse"."public"."datos_clima"
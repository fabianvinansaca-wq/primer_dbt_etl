-- Modelo staging para calidad del aire
SELECT
    ciudad_id AS nombre_ciudad,          -- Renombrado solicitado para homologar con clima
    indice_ica,
    particulas_pm25
FROM "db_warehouse"."public"."calidad_aire"
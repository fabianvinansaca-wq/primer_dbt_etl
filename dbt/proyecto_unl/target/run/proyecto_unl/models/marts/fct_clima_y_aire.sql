
  
    

  create  table "db_warehouse"."public"."fct_clima_y_aire__dbt_tmp"
  
  
    as
  
  (
    

SELECT
    c.nombre_ciudad,
    c.temperatura,
    
ROUND((c.temperatura * 9.0 / 5.0) + 32.0, 2)
 AS temperatura_fahrenheit,
    c.clima,
    a.indice_ica,
    a.particulas_pm25,
    
CASE
    WHEN a.indice_ica <= 50 THEN 'Bueno'
    WHEN a.indice_ica > 50 AND a.indice_ica <= 100 THEN 'Moderado'
    WHEN a.indice_ica > 100 THEN 'Dañino'
    ELSE 'Sin clasificar'
END
 AS categoria_calidad_aire
FROM "db_warehouse"."public"."stg_clima" c
INNER JOIN "db_warehouse"."public"."stg_calidad_aire" a
    ON c.nombre_ciudad = a.nombre_ciudad
  );
  
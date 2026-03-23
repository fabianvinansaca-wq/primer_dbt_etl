
  
    

  create  table "db_warehouse"."public"."fct_resumen_clima__dbt_tmp"
  
  
    as
  
  (
    

SELECT
    nombre_ciudad,
    temperatura,
    
ROUND((temperatura * 9.0 / 5.0) + 32.0, 2)
 AS temperatura_fahrenheit,
    clima
FROM "db_warehouse"."public"."stg_clima"
  );
  
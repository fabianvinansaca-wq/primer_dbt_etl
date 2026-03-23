import os
import subprocess
from datetime import datetime
from airflow.decorators import dag, task
from tenacity import retry, stop_after_attempt, wait_exponential

# ==========================================
# 1. FUNCIÓN DE RESILIENCIA (TENACITY)
# ==========================================
@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))

def ejecutar_dbt_resiliente(comando: str, directorio: str):

    print(f"Ejecutando: {comando} en {directorio}")

    resultado = subprocess.run(
        comando, cwd=directorio, shell=True, capture_output=True, text=True
    )

    print("STDOUT:\n", resultado.stdout)
    print("STDERR:\n", resultado.stderr)
    if resultado.returncode != 0:
        print(f"Error dbt:\n{resultado.stderr}\n{resultado.stdout}")
        raise Exception(f"Fallo en: {comando}")
    
    print(f"Éxito:\n{resultado.stdout}")
    return True

# ==========================================
# 2. DEFINICIÓN DEL DAG (TASKFLOW API)
# ==========================================
@dag(
    dag_id='Foro_dag_dbt_U_taller_ETL',
    description='Generación y ejecución dinámica de dbt con Jinja y Tenacity- Tarea - Foro',
    schedule_interval=None, 
    start_date=datetime(2026, 2, 10),
    catchup=False,
    tags=['ETL', 'dbt', 'Jinja', 'UNL','Foro']
)

def taller_semana10_flujo():
    
    DBT_DIR = "/opt/airflow/dbt/proyecto_unl"

    # TAREA 1: Generar toda la estructura y código SQL con Jinja dinámicamente
    @task
    def desplegar_codigo_dbt():
        # Definir rutas
        dirs = {
            "staging": os.path.join(DBT_DIR, "models", "staging"),
            "marts": os.path.join(DBT_DIR, "models", "marts"),
            "macros": os.path.join(DBT_DIR, "macros"),
            "seeds": os.path.join(DBT_DIR, "seeds")
        }
        
        # Crear directorios si no existen
        for d in dirs.values():
            os.makedirs(d, exist_ok=True)

        # 1. Crear Semilla (CSV)
        # Datos Clima

        clima_seed_path = f"{DBT_DIR}/seeds/datos_clima.csv"
        with open(clima_seed_path, "w", encoding="utf-8") as f:
            f.write(
                "ciudad,temperatura,clima\n"
                "Loja,22,Soleado\n"
                "Quito,18,Nublado\n"
                "Guayaquil,30,Caluroso\n"
                "Cuenca,16,Lluvioso\n"
            )
        # Datos Calidad de Aire
        
        calidad_aire_seed_path = f"{DBT_DIR}/seeds/calidad_aire.csv"
        with open(calidad_aire_seed_path, "w", encoding="utf-8") as f:
            f.write(
                "ciudad_id,indice_ica,particulas_pm25\n"
                "Loja,45,12.5\n"
                "Quito,110,35.2\n"
                "Guayaquil,85,25.0\n"
                "Cuenca,55,15.1\n"
            )

        # ARCHIVO sources.yml - Agregando la tabla de calidad_aire
        sources_yml_path = f"{DBT_DIR}/models/staging/sources.yml"
        with open(sources_yml_path, "w", encoding="utf-8") as f:
            f.write(
                """version: 2

sources:
  - name: raw_data
    schema: public
    tables:
      - name: datos_clima
      - name: calidad_aire
"""
            )
        # MODELO STAGING ORIGINAL: stg_clima.sql
        stg_clima_path = f"{DBT_DIR}/models/staging/stg_clima.sql"
        with open(stg_clima_path, "w", encoding="utf-8") as f:
            f.write(
                """-- Modelo staging para datos climáticos
SELECT
    ciudad AS nombre_ciudad,             -- Estandarización del nombre de columna
    temperatura,
    clima
FROM {{ source('raw_data', 'datos_clima') }}
"""
            )
        # =====================================================
        # 5. NUEVO MODELO STAGING: stg_calidad_aire.sql
        # CAMBIO REALIZADO:
        # Se crea el modelo solicitado usando source()
        # y se renombra ciudad_id -> nombre_ciudad para hacer JOIN posterior.
        # =====================================================
        stg_calidad_aire_path = f"{DBT_DIR}/models/staging/stg_calidad_aire.sql"
        with open(stg_calidad_aire_path, "w", encoding="utf-8") as f:
            f.write(
                """-- Modelo staging para calidad del aire
SELECT
    ciudad_id AS nombre_ciudad,          -- Renombrado solicitado para homologar con clima
    indice_ica,
    particulas_pm25
FROM {{ source('raw_data', 'calidad_aire') }}
"""
            )
        # =====================================================
        # 6. MACRO EXISTENTE O DE EJEMPLO 
        # =====================================================
        macro_temp_path = f"{DBT_DIR}/macros/celsius_a_fahrenheit.sql"
        with open(macro_temp_path, "w", encoding="utf-8") as f:
            f.write(
                """{% macro celsius_a_fahrenheit(col) %}
ROUND(({{ col }} * 9.0 / 5.0) + 32.0, 2)
{% endmacro %}
"""
            )
        # =====================================================
        # 7. NUEVA MACRO: clasificacion_aire.sql
        # CAMBIO REALIZADO:
        # Implementa principio DRY para clasificar el índice ICA.
        # =====================================================
        macro_calidad_aire_path = f"{DBT_DIR}/macros/clasificacion_aire.sql"
        with open(macro_calidad_aire_path, "w", encoding="utf-8") as f:
            f.write(
                """{% macro clasificacion_aire(col) %}
CASE
    WHEN {{ col }} <= 50 THEN 'Bueno'
    WHEN {{ col }} > 50 AND {{ col }} <= 100 THEN 'Moderado'
    WHEN {{ col }} > 100 THEN 'Dañino'
    ELSE 'Sin clasificar'
END
{% endmacro %}
"""
            )
        
         # =====================================================
        # 8. MODELO MART ANTERIOR (si deseas mantenerlo)
        # =====================================================
        mart_resumen_clima_path = f"{DBT_DIR}/models/marts/fct_resumen_clima.sql"
        with open(mart_resumen_clima_path, "w", encoding="utf-8") as f:
            f.write(
                """{{ config(materialized='table') }}

SELECT
    nombre_ciudad,
    temperatura,
    {{ celsius_a_fahrenheit('temperatura') }} AS temperatura_fahrenheit,
    clima
FROM {{ ref('stg_clima') }}
"""
            )
         # =====================================================
        # 9. NUEVO MART FINAL: fct_clima_y_aire.sql
        # CAMBIO REALIZADO:
        # - usa ref() para invocar stg_clima y stg_calidad_aire
        # - hace JOIN por nombre_ciudad
        # - usa macro clasificacion_aire()
        # - se materializa como tabla física
        # =====================================================
        mart_clima_aire_path = f"{DBT_DIR}/models/marts/fct_clima_y_aire.sql"
        with open(mart_clima_aire_path, "w", encoding="utf-8") as f:
            f.write(
                """{{ config(materialized='table') }}

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
"""
            )

        return "Estructura dbt creada correctamente"

    # TAREA 2: Sembrar datos
    @task
    def sembrar_datos():
        return ejecutar_dbt_resiliente("dbt seed", DBT_DIR)

    # TAREA 3: Ejecutar Transformaciones
    @task
    def ejecutar_transformaciones():
        return ejecutar_dbt_resiliente("dbt run", DBT_DIR)
    
    @task
    def ejecutar_pruebas():
        return ejecutar_dbt_resiliente("dbt test", DBT_DIR)

    # ==========================================
    # LINAJE DE TAREAS (DEPENDENCIAS)
    # ==========================================
    desplegar_codigo_dbt() >> sembrar_datos() >> ejecutar_transformaciones() >> ejecutar_pruebas()

# Instanciar el DAG
dag_principal = taller_semana10_flujo()
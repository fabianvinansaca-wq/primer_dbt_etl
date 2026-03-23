{% macro clasificacion_aire(col) %}
CASE
    WHEN {{ col }} <= 50 THEN 'Bueno'
    WHEN {{ col }} > 50 AND {{ col }} <= 100 THEN 'Moderado'
    WHEN {{ col }} > 100 THEN 'Dañino'
    ELSE 'Sin clasificar'
END
{% endmacro %}

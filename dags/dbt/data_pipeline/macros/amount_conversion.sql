{% macro penny_to_pounds(amount, scale=2) %}
    (0.01 * {{ amount }})::numeric(16, {{ scale }})
{% endmacro %}

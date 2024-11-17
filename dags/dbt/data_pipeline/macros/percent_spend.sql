{% macro percent_income_spend(sub_total, scale=2) %}
    (({{ sub_total }} / 630)*100)::numeric(16, {{ scale }})
{% endmacro %}




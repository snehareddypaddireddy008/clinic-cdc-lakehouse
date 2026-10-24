{#- Patient contact details never reach the marts in clear text. -#}

{% macro hash_email(column) %}
    case when {{ column }} is null then null else md5(lower(trim({{ column }}))) end
{% endmacro %}

{% macro mask_phone(column) %}
    case when {{ column }} is null then null else '***-***-' || right(regexp_replace({{ column }}, '[^0-9]', '', 'g'), 4) end
{% endmacro %}

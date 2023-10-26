{% macro markup(sellingprice,costprice) %}
({{sellingprice}}-{{ordercostprice}})/{{ordercostprice}}
{% endmacro %}
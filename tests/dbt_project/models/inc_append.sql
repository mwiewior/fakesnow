{{ config(materialized='incremental', incremental_strategy='append') }}
select * from {{ ref('stg_orders') }}
{% if is_incremental() %} where updated_at > (select max(updated_at) from {{ this }}) {% endif %}

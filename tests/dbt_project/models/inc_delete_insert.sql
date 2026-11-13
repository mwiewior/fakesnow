{{ config(materialized='incremental', incremental_strategy='delete+insert', unique_key='id') }}
select * from {{ ref('stg_orders') }}
{% if is_incremental() %} where updated_at >= (select max(updated_at) from {{ this }}) {% endif %}

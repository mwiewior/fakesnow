{{ config(materialized='view') }}
select id, customer, amount, updated_at from {{ ref('raw_orders') }}

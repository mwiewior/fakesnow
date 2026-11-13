{{ config(materialized='table') }}
select customer, sum(amount) as total from {{ ref('stg_orders') }} group by customer

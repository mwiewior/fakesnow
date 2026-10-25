{% snapshot orders_snap %}
{{ config(target_schema='SNAPSHOTS', unique_key='id', strategy='timestamp', updated_at='updated_at') }}
select * from {{ ref('stg_orders') }}
{% endsnapshot %}

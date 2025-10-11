{% macro audit_table_creation() %}
    -- Create audit table to track model runs
    create table if not exists {{ target.schema }}_audit.model_runs (
        run_id varchar,
        model_name varchar,
        run_started_at timestamp,
        run_completed_at timestamp,
        rows_affected integer,
        status varchar
    );
    
    insert into {{ target.schema }}_audit.model_runs (
        run_id,
        model_name,
        run_started_at,
        run_completed_at,
        status
    ) values (
        '{{ invocation_id }}',
        'dbt_run',
        '{{ run_started_at }}',
        current_timestamp,
        'completed'
    );
{% endmacro %}

{% test is_even(model, column_name) %}
    -- Custom test to check if values are even
    select *
    from {{ model }}
    where {{ column_name }} % 2 != 0
{% endtest %}
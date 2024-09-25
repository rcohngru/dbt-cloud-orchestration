{% macro build_orchestration_resources(prod_override=False) %}
    {#
        By default, the tasks built into a development environment will have the CALL PROCEDURE
        statement ommitted. This is out of an abundance of caution. Calling this macro with
        prod_override set to True will build the tasks with the CALL PROCEDURE statement
        included. Regardless of the prod_override parameter, any tasks built into a development
        environment will build in a suspended state and must be enabled manually by the user.
    #}

{% if execute %}
    {% set create_schema_sql %}
        create schema if not exists {{target.schema}};
    {% endset %}

    {% do log('>> Building resources into schema: ' ~ target.schema, True) %}
    {% set use_schema_sql %}
        use schema {{target.schema}};
    {% endset %}

    {% set create_log_table_sql -%}
        create or replace table {{target.database}}.{{target.schema}}.orchestration_logs(
            job_name string,
            job_id integer,
            task_name string,
            invocation_time timestamp_tz,
            invocation_status string,
            logs array
        );
    {%- endset %}

    {% do run_query(create_schema_sql) %}
    {% do run_query(use_schema_sql) %}
    {% do run_query(create_log_table_sql) %}

    {# Create SPROC #}
    {% do create_proc_dbtcloud_run_job() %}

    {{ create_stream_generic(
      database = "<DATABASE>",
      schema = "<SCHEMA>",
      table = "<TABLE>"
    )}}

    {{ create_task_generic(
      job_id = ######,
      job_name = "dbt Cloud Job Name",
      streams = ["dbt_orchestration_stream__<SCHEMA>__<TABLE>"],
      prod_override = prod_override
    )}}

{% endif %}

{% endmacro %}

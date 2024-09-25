/*
 * Given a database, schema, and table, will create a Snowflake Stream to track changes on table.
 *
 * Required Parameters:
 *  - database (string): the name of the database in which the stream will live
 *  - schema (string): the name of the schema in which the stream will live
 *  - table (string): the name of the table in which the stream will live
 *
 * Usage Notes:
 *   - This macro can be used for case-sensitive tables as well by escaping the quotation mark when the argument is passed in.
 *   - The stream created in this macro will build into the TARGET SCHEMA of the dbtCloud environment
 *
 * Examples:
 * - dbt run-operation create_stream_generic --args '{"database":"db_name","schema":"schema_name","table":"table_name"}'
 *   - creates a stream called 'dbt_orchestration_stream_table_name' that tracks db_name.schema_name.table_name
 *
 * - dbt run-operation create_stream_generic --args '{"database":"db_name","schema":"schema_name","table":"\"TABLE_NAME\""}'
 *   - creates a stream called 'dbt_orchestration_stream_table_name' that tracks db_name.schema_name."TABLE_NAME"
 */

{% macro create_stream_generic(database, schema, table) %}

    {# validate required arguments #}
    {% set missing_args = [] %}
    {% if database | length == 0 or not database %}
        {% set _ = missing_args.append("database") %}
    {% endif %}
    {% if schema | length == 0 or not schema %}
        {% set _ = missing_args.append("schema") %}
    {% endif %}
    {% if table | length == 0 or not table %}
        {% set _ = missing_args.append("table") %}
    {% endif %}

    {% if missing_args | length > 0 %}
        {% do exceptions.raise_compiler_error('Macro create_stream_generic missing required parameters: ' ~ missing_args | join(", ")) %}
    {% endif %}

    {# create names for stream #}
    {% set stream_name -%}
        dbt_orchestration_stream__{{schema | lower}}__{{table | replace('"', '') | lower }}
    {%- endset %}

    {% set fully_qualified_name -%}
        {{database}}.{{schema}}.{{table}}
    {%- endset %}

    {{ log(">> Creating Snowflake Stream `" ~ stream_name ~ "` to track changes on " ~ fully_qualified_name, True) }}

    {# create stream #}
    {% set stream_statement -%}
        create or replace stream {{stream_name}}
        on table {{fully_qualified_name}};
    {%- endset %}

    {% do run_query(stream_statement) %}


{% endmacro %}

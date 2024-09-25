/*
 * Given a series or parameters, will create a Snowflake Task.
 *
 * Required Parameters:
 *  - job_id (int): the id of the dbtCloud job to be triggered.
 *  - job_name (string): the name of the dbtCloud job to be triggered
 *  - streams (List[string]): a list of streams to be checked in order to determine if the task should trigger
 *
 * Optional Parameters:
 *  - schedule (string) = '15 MINUTE' : the schedule that the task runs on. Can be CRON or MINUTE, per the Snowflake Task documentation
 *  - upstream_job_id (int) = -1 : the id of an upstream dbtCloud job id to be checked prior to job triggering. If no upstream jobs, use -1
 *  - min_elapsed_seconds (int) = 300 : the minimum number of seconds that must pass after the last run before the dbtCloud job is eligible to trigger
 *  - min_start_time (string) = '00:00' : the time of the day (HH:MM) after which the dbtCloud job is eligible to trigger
 *  - max_start_time (string) = '23:59' : the time of the day (HH:MM) before which the dbtCloud job is eligible to trigger
 *  - days_of_week (List[int]) = [0,1,2,3,4,5,6] : a list of weekdays (Mon = 0, Tue = 1, etc...) on which the dbtCloud job is eligible to trigger
 *  - prod_override (bool) = False : a boolean parameter that will allow the user to add in the `CALL PROCEDURE` statement despite not being in a production environment
 *
 * Usage Notes:
 * - The 'streams' parameter should be just the name of the stream itself, and not the fully qualified name. The macro is designed to leverage
 * the TARGET SCHEMA of the dbt environment.
 * - All tasks will be created in a suspended state, unless the Target environment is production.
 */

{% macro create_task_generic(
    job_id,
    job_name,
    streams,
    schedule='15 MINUTE',
    upstream_job_id=-1,
    min_elapsed_seconds=300,
    min_start_time='00:00',
    max_start_time='23:59',
    days_of_week=[0,1,2,3,4,5,6],
    prod_override=False,
    logging_table='orchestration_logs'
) %}

    {# validate required arguments #}


    {% set missing_args = [] %}
    {% if not job_id %}
        {% set _ = missing_args.append("job_id") %}
    {% endif %}
    {% if not job_name %}
        {% set _ = missing_args.append("job_name") %}
    {% endif %}
    {% if streams | length == 0 or not streams %}
        {% set _ = missing_args.append("streams") %}
    {% endif %}

    {% if missing_args | length > 0 %}
        {% do exceptions.raise_compiler_error('Macro create_task_generic missing required parameters: ' ~ missing_args | join(", ")) %}
    {% endif %}


    {% set task_name -%}
        dbt_orchestration_task__{{job_name}}__{{job_id}}
    {%- endset %}

    {% set task_conditions = [] %}
    {% set stream_resets = [] %}

    {% for stream in streams %}

        {% set stream_fqn = target.database ~ '.' ~ target.schema ~ '.' ~ stream %}

        {% set cond_stmt %}
            system$stream_has_data('{{stream_fqn}}')
        {%- endset %}

        {% set _ = task_conditions.append(cond_stmt) %}

        {% set clear_stmt %}
            create or replace temp table {{stream}}_reset_temp as
            select * from {{stream_fqn}};
        {%- endset %}

        {% set _ = stream_resets.append(clear_stmt)%}

    {% endfor %}

    {% set condition = task_conditions | join(' or ') %}

    {% set clear_streams = stream_resets | join('') %}

    {{ log(">> Creating Snowflake Task `" ~ task_name ~ "`", True) }}

    {% set call_proc_stmt -%}
        call {{target.database}}.{{target.schema}}.dbtcloud_run_job(
            job_id => {{job_id}},
            upstream_job_id => {{upstream_job_id}},
            min_elapsed_seconds => {{min_elapsed_seconds}},
            min_start_time => '{{min_start_time}}',
            max_start_time => '{{max_start_time}}',
            days_of_week => {{days_of_week}}
        );
    {%- endset %}

    {% set logging_stmt -%}
        insert into {{target.database}}.{{target.schema}}.{{logging_table}}(job_name, job_id, task_name, invocation_time, invocation_status, logs)
        select
            '{{job_name}}' as job_name,
            {{job_id}} as job_id,
            '{{task_name}}' as task_name,
            :invocation_time as invocation_time,
            parse_json(dbtcloud_run_job)['invoked'] as invocation_status,
            parse_json(dbtcloud_run_job)['logs'] as logs,
        from table(result_scan(last_query_id()));
    {%- endset %}

    {% set task_stmt -%}

        create or replace task {{target.database}}.{{target.schema}}.{{task_name}}
        schedule='{{schedule}}'
        warehouse=<ENTER WAREHOUSE NAME>
        when (
            {{condition}}
        )
        as execute immediate
        $$
        declare
            invocation_time timestamp_tz;
        begin
            {{clear_streams}}

            invocation_time := current_timestamp();

            {% if target.name == "prod" or prod_override %}
                {{call_proc_stmt}}
                {{logging_stmt}}
            {% endif %}
        end;
        $$;

    {% if target.name == "prod" %}
        {{ log("Production environment detected, resuming task...", True) }}
        alter task {{target.database}}.{{target.schema}}.{{task_name}} resume;
    {% endif %}

    {%- endset %}

    {% do run_query(task_stmt) %}

{% endmacro %}

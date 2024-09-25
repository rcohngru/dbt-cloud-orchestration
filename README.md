# dbt-cloud-orchestration
The `.sql` files located in the [code](./code/) directory of this repo contain
Jinja macros for use in the creation of orchestration resources in Snowflake, as
described in the Orchestration as Code presentation given at Coalesce 2024.

# Usage
### `external_access_integration.sql`
The SQL code in this file creates a series of resources in Snowflake that are necessary for the rest of these tools to work. They are one-time creations that can be done by hand in the Snowflake UI.

They allow the python stored procedure to access endpoints external from Snowflake -- in this case the dbt Cloud API.

### `build_orchestration_resources.sql`
This Jinja macro is the only macro that should be run by hand from the dbt Cloud console. When run, this macro invokes a series of other macros, creating all of the orchestration resources necessary for dynamic job rebuilding.

This macro can be run once, however I recommend configuring a weekly dbt Cloud job to run this macro, in case any resources in Snowflake are accidentally deleted.

It can be run as follows:
`dbt run-operation build_orchestration_resources`

### `create_proc__dbt_cloud_run_job.sql`
This Jinja macro creates a python stored procedure in Snowflake. When invoked, this stored procedure will perform a series of checks before eventually triggering a dbt Cloud job.

Each of these checks is optional, however if any of them fail the job will not be triggered. The checks are listed as follows:
- `upstream_job_id`: if, the code will verify that the job tied to the id has already completed at least once in the current day
- `min_elapsed_seconds`: if given, the code will only trigger if it has been longer than the specified minimum number of seconds since the job was last run
- `min_start_time`: if given, the code will only trigger if the time is currently after the minimum start time
- `max_start_time`: if given, the code will only trigger if the time is currently before the maximum start time
- `days_of_week`: if given, the code will only trigger if the current day is in the specified days of week list

One of the drawbacks of python stored procedures in Snowflake is that there is not an easy way to view logs or print statements. To get around this we use a list and continually append our log statements, which is ultimately returned upon procedure completion.

### `create_stream_generic.sql`
This Jinja macro will create a stream that tracks the specified table.
Note: change tracking must be enabled on this table prior to use.

### `create_task_generic.sql`
This Jinja macro will create a task that runs on a specified schedule, and attempts to invoke the `dbt_cloud_run_job` stored procedure.

For each run, the task will first check to see if any of the specified streams has data in them. If any stream does have data, the task will trigger.

The body of the task executes 3 sections of statements.

The first section is a series of DML operations on the streams that "clears" them out. This ensures that the task will not continue to trigger on every run.

The second section invokes the stored procedure that triggers a dbt Cloud job. The parameters that are passed to the macro are subsequently passed into the stored procedure and are used for the conditional triggering described above.

The third section inserts the logs returned from the procedure call into a table that is specified.

Finally, there is a second statement that is submitted to Snowflake in addition to the task. This statement is optional, it will only be passed on a production run unless it is overridden in the `dbt run-operation` command. This statement resumes the task, since all tasks are in a suspended state on creation.

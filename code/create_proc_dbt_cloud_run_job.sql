{% macro create_proc_dbtcloud_run_job() %}

{% set create_proc_stmt %}

create or replace procedure {{target.database}}.{{target.schema}}.dbtcloud_run_job(
    job_id integer,
    upstream_job_id integer default -1,
    min_elapsed_seconds integer default 300,
    min_start_time string default '00:00',
    max_start_time string default '23:59',
    days_of_week array default [0,1,2,3,4,5,6]
)
    returns string
    language python
    runtime_version = '3.11'
    packages = ('snowflake-snowpark-python', 'requests', 'pytz', 'typing')
    handler = 'handler'
    external_access_integrations = (DBT_CLOUD_EXTERNAL_ACCESS_INTEGRATION)
    secrets = ('access_token' = <DATABASE>.<SCHEMA>.DBT_CLOUD_ACCESS_TOKEN)
as
$$
import _snowflake
import requests
import pytz
from datetime import datetime
from typing import List

DATETIME_FMT = "%Y-%m-%d %H:%M:%S.%f%z"
DATE_FMT = "%Y-%m-%d"
EPOCH_TS = "1970-01-01 00:00:00.000000+00:00"
LOCAL_TZ = pytz.timezone("US/Central")
NOW = datetime.now(LOCAL_TZ)
DBT_STATUS_DICT = {
    1: "Queued",
    2: "Starting",
    3: "Running",
    10: "Success",
    20: "Error",
    30: "Cancelled",
}

def validate_time_filter(min_start_time, max_start_time, days_of_week):
    """
    If the mapping file for this look contained a time filter, verify that the
    current running time of the lambda falls within the specified range.
    """
    logs: List[str] = []

    min_start_time = datetime.strptime(min_start_time, "%H:%M").time()
    max_start_time = datetime.strptime(max_start_time, "%H:%M").time()

    current_datetime = datetime.now(LOCAL_TZ)
    current_time = current_datetime.time()
    current_weekday = current_datetime.weekday()

    if current_time < min_start_time or current_time > max_start_time:
        logs.append(
            f"Current time is {current_time}, which falls outside of the specified acceptable range of: {min_start_time} to {max_start_time}. Will not kick off"
        )
        return False, logs

    if current_weekday not in days_of_week:
        logs.append(
            f"Current weekday is {current_weekday}, which is not in the list of accepted weekdays: {days_of_week}. Will not kick off"
        )
        return False, logs

    logs.append(
        f"The current datetime is {current_datetime}, which falls within the specified kickoff filter of '{min_start_time}' to '{max_start_time}' on days {days_of_week}."
    )
    return True, logs

def validate_upstream_dependencies(session: requests.Session, account_id: int, upstream_dbt_job_id: int):
    """
    Checks the last run of the upstream dbt job to confirm that it ran successfully
    at least once on the current day.
    """
    logs: List[str] = []

    if upstream_dbt_job_id == -1:
        logs.append("No upstream dependencies")

        return True, logs

    params = {
        "job_definition_id": upstream_dbt_job_id,
        "order_by": "-created_at",
        "limit": 1,
        "offset": 0
    }
    response = session.get(f"https://cloud.getdbt.com/api/v2/accounts/{account_id}/runs/", params=params)
    run = response.json().get("data")[0]

    last_run_status = DBT_STATUS_DICT.get(run.get("status"))

    last_run_created_at = datetime.strptime(
        run.get("created_at") or EPOCH_TS, DATETIME_FMT
    )
    last_run_created_at_central = last_run_created_at.replace(
        tzinfo=pytz.utc
    ).astimezone(tz=LOCAL_TZ)

    run_id = run.get("id")

    logs.append(f"Retrieved data from the most recently created run (ID# {run_id}) for upstream job {upstream_dbt_job_id}")


    logs.append(f"Upstream Run # {run_id} created at {last_run_created_at_central.strftime(DATETIME_FMT)}")
    if last_run_created_at.strftime(DATE_FMT) != NOW.strftime(DATE_FMT):
        logs.append(f"Last upstream run was not on same day as {NOW.strftime(DATE_FMT)}, will not kick off")
        return False, logs

    logs.append(f"Run # {run_id} status: {last_run_status}")
    if last_run_status != "Success":
        logs.append(f"Upstream run was not a Success, will not kick off")
        return False, logs

    return True, logs

def validate_prior_run(session: requests.Session, account_id: int, job_id: int, min_elapsed_seconds: int):

    logs: List[str] = []

    params = {
        "job_definition_id": job_id,
        "order_by": "-created_at",
        "limit": 1,
        "offset": 0
    }
    response = session.get(f"https://cloud.getdbt.com/api/v2/accounts/{account_id}/runs/", params=params)
    run = response.json().get("data")[0]

    last_run_status = DBT_STATUS_DICT.get(run.get("status"))

    last_run_created_at = datetime.strptime(
        run.get("created_at") or EPOCH_TS, DATETIME_FMT
    )
    last_run_created_at_central = last_run_created_at.replace(
        tzinfo=pytz.utc
    ).astimezone(tz=LOCAL_TZ)

    elapsed = NOW - last_run_created_at_central
    elapsed_seconds = elapsed.total_seconds()
    run_id = run.get("id")
    logs.append(f"Retrieved data from the most recently created run (ID# {run_id})")

    logs.append(f"Run # {run_id} created at {last_run_created_at_central.strftime(DATETIME_FMT)}, {elapsed_seconds} prior to the current time of {NOW.strftime(DATETIME_FMT)}")
    if elapsed_seconds < min_elapsed_seconds:
        logs.append(f"Elapsed seconds of {elapsed_seconds} is less than the minimum of {min_elapsed_seconds} seconds, will not kick off")
        return False, logs

    logs.append(f"Run # {run_id} status: {last_run_status}")
    if last_run_status == "Queued":
        logs.append(f"Prior run is in a 'Queued' state, will not kick off")
        return False, logs

    return True, logs

def handler(
        session,
        job_id: int,
        upstream_job_id: int,
        min_elapsed_seconds: int,
        min_start_time: str,
        max_start_time: str,
        days_of_week: List[int]
    ):

    access_token = json.loads(_snowflake.get_generic_secret_string("access_token"))["access_token"]
    account_id = '...'

    session = requests.Session()
    session.headers.update({
        "Authorization": f"Token {access_token}"
    })

    logs: List[str] = []

    result: bool
    validation_logs: List[str]

    result, validation_logs = validate_prior_run(session, account_id, job_id, min_elapsed_seconds)
    logs.extend(validation_logs)
    if not result:
        return {"invoked": "FAILED", "logs": logs}

    result, validation_logs = validate_upstream_dependencies(session, account_id, upstream_job_id)
    logs.extend(validation_logs)
    if not result:
        return {"invoked": "FAILED", "logs": logs}

    result, validation_logs = validate_time_filter(min_start_time, max_start_time, days_of_week)
    logs.extend(validation_logs)
    if not result:
        return {"invoked": "FAILED", "logs": logs}

    logs.append("All checks have passed. Will kick off job")
    url = f"https://cloud.getdbt.com/api/v2/accounts/{account_id}/jobs/{job_id}/run/"

    data = {
        "cause": "Kicked off via Snowflake Task."
    }

    session.post(url=url, data=data)

    return {"invoked": "SUCCESS", "logs": logs}

$$;

{% endset %}

{{ log('>> Creating DBTCLOUD_RUN_JOB() stored procedure...', True) }}

{% do run_query(create_proc_stmt) %}

{% endmacro %}

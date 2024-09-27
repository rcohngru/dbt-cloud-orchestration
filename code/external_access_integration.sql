/*
  Create network rule.
  This allows egress outside of snowflake.
  This network rule is used by the external access integration to hit the dbt cloud api.
*/
create network rule <DATABASE>.<SCHEMA>.DBT_CLOUD_EGRESS
  type = host_port
  mode = egress
  value_list = ('cloud.getdbt.com')
;

/*
  Create secret.
  This secret holds the access token used to authenticate with the dbt cloud api.
*/
create secret <DATABASE>.<SCHEMA>.DBT_CLOUD_ACCESS_TOKEN
    TYPE = GENERIC_STRING
    SECRET_STRING = '{"access_token":"..."}'
;

/*
  This integration allows a stored procedure to access the dbt cloud access token
  and submit requests outside of Snowflake.
*/
create external access integration DBT_CLOUD_EXTERNAL_ACCESS_INTEGRATION
  allowed_network_rules = (DBT_CLOUD_EGRESS)
  allowed_authentication_secrets = ('<DATABASE>.<SCHEMA>.DBT_CLOUD_ACCESS_TOKEN')
  enabled = true
;



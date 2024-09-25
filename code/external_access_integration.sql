create network rule <DATABASE>.<SCHEMA>.DBT_CLOUD_EGRESS
  type = host_port
  mode = egress
  value_list = ('cloud.getdbt.com')
;

create secret <DATABASE>.<SCHEMA>.DBT_CLOUD_ACCESS_TOKEN
    TYPE = GENERIC_STRING
    SECRET_STRING = '{"access_token":"..."}'
;

create external access integration DBT_CLOUD_EXTERNAL_ACCESS_INTEGRATION
  allowed_network_rules = (DBT_CLOUD_EGRESS)
  allowed_authentication_secrets = ('<DATABASE>.<SCHEMA>.DBT_CLOUD_ACCESS_TOKEN')
  enabled = true
;



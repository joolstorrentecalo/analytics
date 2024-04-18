##### New User Steps
1. [ ] Link to Snowflake AR: \<>
1. [ ] Update [permissions/snowflake/snowflake_users.yml](https://gitlab.com/gitlab-data/analytics/-/blob/master/permissions/snowflake/snowflake_users.yml?ref_type=heads) with new user(s), then push changes
1. [ ] Run below manual CI jobs (can be run **concurrently**):
    - [ ] Stage :snake: Python `snowflake_provisioning_snowflake_users`
    - [ ] Stage :snake: Python: `snowflake_provisioning_roles_yaml`
1. [ ] Check automatic CI jobs have passed
    - [ ] Stage :snake: Python: `📁yaml_validation`
    - [ ] Stage :snake: Python: `🧊permifrost_spec_test`
1. [ ] Assign to CODEOWNER for review

##### Reviewer Steps
1. [ ] check if MR is in line with linked Access Request.
1. [ ] Merge MR
1. [ ] Add or remove email(s) in the [Snowflake Okta Google Group](https://groups.google.com/a/gitlab.com/g/okta-snowflake-users/members?pli=1).

##### Runbook
Refer to the [Runbook](https://gitlab.com/gitlab-data/runbooks/-/blob/main/snowflake_provisioning_automation/snowflake_provisioning_automation.md) for more details

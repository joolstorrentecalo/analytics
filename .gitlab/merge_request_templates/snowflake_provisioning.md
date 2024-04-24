##### New User Steps

1. [ ] Link to Snowflake AR: \<>
1. [ ] Update [permissions/snowflake/snowflake_users.yml](https://gitlab.com/gitlab-data/analytics/-/blob/master/permissions/snowflake/snowflake_users.yml?ref_type=heads) with new user(s), then push changes
1. [ ] Run CI job: Stage :snake: Python: `snowflake_provisioning_roles_yaml`
1. [ ] Assign to CODEOWNER for review

##### Reviewer Steps

1. [ ] Approve MR after checking if MR is in line with linked Access Request
1. [ ] Manually trigger new CI pipeline to unlock CI jobs:
    - [ ] Run CI job: Stage :snake: Python `snowflake_provisioning_snowflake_users`
    - [ ] Run CI job: Stage :snake: Python: `🧊permifrost_spec_test`
1. [ ] Merge MR
1. [ ] Add or remove email(s) in the [Snowflake Okta Google Group](https://groups.google.com/a/gitlab.com/g/okta-snowflake-users/members?pli=1)


##### Runbook
Refer to the [Runbook](https://gitlab.com/gitlab-data/runbooks/-/blob/main/snowflake_provisioning_automation/snowflake_provisioning_automation.md) for more details



/label ~"Priority::1-Ops" ~"Team::Data Platform"  ~Snowflake ~Provisioning 
For more information, see:
- [Handbook](https://handbook.gitlab.com/handbook/business-technology/data-team/platform/#snowflake-provisioning-automation)
- [Runbook](https://gitlab.com/gitlab-data/runbooks/-/blob/main/snowflake_provisioning_automation/snowflake_provisioning_automation.md?ref_type=heads)

### Additional development notes

Useful terminology within the code:
- `email`: The email of the user, i.e `jdoe-ext@gitlab.com`
- `user`: The email of the user, but *without* the `@gitlab.com`, i.e `jdoe-ext`
- `username`: The snowflake username, certain characters are dropped i.e `jdoe`

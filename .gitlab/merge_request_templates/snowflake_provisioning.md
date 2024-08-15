##### Add/Remove Users

1. [ ] Link to Snowflake AR: \<>
1. [ ] Update [permissions/snowflake/snowflake_users.yml](https://gitlab.com/gitlab-data/analytics/-/blob/master/permissions/snowflake/snowflake_users.yml?ref_type=heads) by adding/removing user(s). Push your changes.
    - The username should not include `@gitlab.com`
    - To minimize merge conflicts, please add users in roughly **alphabetical order**. If you need to add multiple users, add the first user alphabetically, and place the remaining users directly below.
1. [ ] Run CI job: Stage :snake: Python: `snowflake_provisioning_roles_yaml`
1. [ ] Assign to CODEOWNER for review

##### Reviewer Steps

1. [ ] Approve MR after checking against linked Access Request.
    - If the user requests a role outside of `snowflake_analyst`, update `roles.yml` manually,  or via CI job [handbook](https://handbook.gitlab.com/handbook/business-technology/data-team/platform/ci-jobs/#further-explanation-1)
1. [ ] Manually trigger new CI pipeline to unlock CI jobs:
    - [ ] Run CI job: Stage :snake: Python `snowflake_provisioning_snowflake_users`
    - [ ] Run CI job: Stage :snake: Python: `🧊permifrost_spec_test`
1. [ ] Merge MR
1. [ ] Add or remove email(s) in the [Snowflake Okta Google Group](https://groups.google.com/a/gitlab.com/g/okta-snowflake-users/members?pli=1)
1. [ ] In the AR, write:
    ```md
    Snowflake provisioning has been completed, changes will take effect at 1:30AM UTC/5:30PM PST.

    Please see this [handbook section](https://handbook.gitlab.com/handbook/business-technology/data-team/platform/#logging-in-and-using-the-correct-role) for logging in.
    ```

##### Runbook

Refer to the [Runbook](https://gitlab.com/gitlab-data/runbooks/-/blob/main/snowflake_provisioning_automation/snowflake_provisioning_automation.md) for more details


/label ~"Priority::1-Ops" ~"Team::Data Platform"  ~Snowflake ~Provisioning

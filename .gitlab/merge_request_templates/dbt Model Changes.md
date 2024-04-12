<!---
  Use this template when making consequential changes to the `/transform` directory,
  including changes to dbt models, tests, seeds, and docs.
--->


Please see the [dbt change workflow](https://handbook.gitlab.com/handbook/business-technology/data-team/how-we-work/dbt-change-workflow/) handbook page for details on these sections.

## Plan

Fill in the subsequent planning sections as part of the preparatory work for these changes.

### Scope

<!--- Link the Issue this MR closes --->
Closes #

This step describes the changes that will be made. Details can be deferred to an issue or epic. Include links to any related MRs and/or issues.

### Tests

Define the tests to check if the changes have the intended impact and the extent of testing and reviews.

### Environment

Define the models that will be needed to construct and test the changes.

## Create

Using the defined plan, set up the local environment, perform the scoped changes, and perform the local testing.
Add any results from the local testing here for documentation.

## Verify

The database and tables created as part of the CI process will act as the remove environment for the changes. 
Using the defined plan, set up the remote environment and perform the remote testing.

### Remote Testing

Add the results of the testing in the remote environment here for review and documentation.

### Review

Before requesting a review consider the following:
- Do the changes require a change to the existing Entity Relationship Diagrams?
- Have the changes been tested sufficiently to have confidence in the effect of the change?
- Have all of the required CI Jobs been run?
- Do the changes meet the team's style and structure standards?
- Have the changes' impact on performance been evaluated?
- Have Masking Policies on columns and their dependencies been considered?
- Has the impact on downstream tables and reports been considered?

#### Accuracy and Effects Review
To be completed by Stakeholders, Subject Matter Experts and Code Owners. Items may be added for user acceptance testing as needed.

- [ ] Checked if a dbt full refresh is needed.
- [ ] Checked if the changes have been tested sufficiently.
- [ ] Checked if the changes have a downstream impact.


#### Compliance Review

To be completed by Maintainers

- [ ] Checked if pipelines have been run and passed.
- [ ] Checked for proper documentation.
- [ ] Checked for impact on performance.
- [ ] Checked changes for alignment to SQL style and structure.
- [ ] Checked if the stated testing has been performed.

## References

<details>
<summary><i>EDM Documentation</i></summary>

Change to EDM models should be reflected on an ERD in the [Lead to Cash ERD Library](https://handbook.gitlab.com/handbook/business-technology/data-team/platform/edw/#lead-to-cash-erds), [Product Release to Adoption ERD Library](https://handbook.gitlab.com/handbook/business-technology/data-team/platform/edw/#product-release-to-adoption-erds), or the [Team Member ERD Library](https://handbook.gitlab.com/handbook/business-technology/data-team/platform/edw/#team-member-erds). Additionally, all columns should be added and defined in the reliant `schema.yml` file.

</details>

<details>
<summary><i>Testing</i></summary>

[Component testing](https://handbook.gitlab.com/handbook/business-technology/data-team/how-we-work/dbt-change-workflow/#component-tests) should be implemented and documented for every model in a `schema.yml` file. At minimum, unique, not nullable fields, and foreign key constraints should be tested, if applicable.
Integration testing should be sufficient to be confident of the effect and impact of the changes. System testing should be used to show the effect and impact of the changes. Acceptance testing should be used to show that the changes meet the needs of the requestor requirements.


</details>

<details>
<summary><i>Style & Structure</i></summary>

- Field names should all be lowercase.
- Function names should all be capitalized.
- All references to existing tables/views/sources (i.e. `{{ ref('...') }}` statements) should be placed in CTEs at the top of the file.
- [dbt Style Guide](https://handbook.gitlab.com/handbook/business-technology/data-team/platform/dbt-guide/#style-and-usage-guide)
- [SQL Style Guide](https://handbook.gitlab.com/handbook/business-technology/data-team/platform/sql-style-guide/#sql-style-guide)

</details>

<details>
<summary><i>Pipelines and Remote Environment Set Up</i></summary>

- The `generate_dbt_docs` job can be used when only the documentation is changed.
- The `build_changes` job should be used for most changes.
- Variables can be added to the `build_changes` job to capture more nuance in the build.
- The `custom_invocation` job can be used to capture anything not covered by the `build_changes` job.
- [dbt CI Jobs](https://handbook.gitlab.com/handbook/business-technology/data-team/platform/ci-jobs/#-dbt-run)

**Which pipeline job do I run?** See the crosswalk our [handbook page](https://handbook.gitlab.com/handbook/business-technology/data-team/platform/ci-jobs/#build_changes) on our CI jobs to better understand which job to run base on what changes are being made.

**What to do for failed pipelines** See our [handbook page](https://handbook.gitlab.com/handbook/business-technology/data-team/platform/ci-jobs/#what-to-do-if-a-pipeline-fails) 


</details>

<details>
<summary><i>Performance</i></summary>


- The [dbt Model Performance runbook](https://gitlab.com/gitlab-data/runbooks/-/blob/main/dbt_performance/model_build_performance.md) will retrieve the performance categories for any changed or new models.
- The [guidelines](https://handbook.gitlab.com/handbook/business-technology/data-team/platform/dbt-guide/#guidance-for-checking-model-performance) in the handbook provide information for improving the performance as needed.

</details>

<details>
<summary><i>Incremental Models</i></summary>

Incremental Models may require a full refresh of the model if the structure of the data or the business logic has changed.  If a full refresh is needed this should be made clear to the reviewer.

</details>

<details>
<summary><i>Salesforce Models</i></summary>


- [ ] Does this MR add, remove, or update logic in `sfdc_account_source`?
  - [ ] Mirror the changes in `sfdc_account_snapshots_source`.
- [ ] Does this MR add, remove, or update logic in `sfdc_opportunity_source`?
  - [ ] Mirror the changes in `sfdc_opportunity_snapshots_source`.
- [ ] Does this MR add, remove, or update logic in `sfdc_users_source`?
  - [ ] Mirror the changes in `sfdc_user_snapshots_source`.
- [ ] Does this MR add, remove, or update logic in `sfdc_account_fields`, `sfdc_user_fields`, or `prep_crm_opportunity`?
  - [ ]If the MR updates these models, a `dbt run --full-refresh` will be needed after merging the MR. Please, add it to the Reviewer Checklist to warn them that this step is required.

</details>

<details>
<summary><i>Schema or Model Name Changes</i></summary>

- If the change introduces a change to the name of a **schema** or **model**, then the impact the the downstream reporting should be evaluated. Also the table with the old model name will need to be dropped from the database, this task can be assigned to the Data Platform team.
- New schemas may require an update to the `data_observability` role permissions that Monte Carlo uses. Guidance for the permissions can be found in the [handbook](https://handbook.gitlab.com/handbook/business-technology/data-team/platform/monte-carlo/#note-on-dwh-permissions) and in MC's official docs.

</details>

<details>

<summary><i>Snapshot Models</i></summary>

If a snapshot model concerns GitLab.com data, make sure it is captured in the selection criteria of the [GDPR deletion macro](https://gitlab.com/gitlab-data/analytics/-/blob/master/transform/snowflake-dbt/macros/warehouse/gdpr_delete_gitlab_dotcom.sql) for GitLab.com data.

</details>

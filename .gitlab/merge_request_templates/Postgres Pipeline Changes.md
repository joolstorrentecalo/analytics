Closes

#### List and Describe Code Changes <!-- focus on why the changes are being made-->

* `change & why it was made`

#### Steps Taken to Test

* Run the ❄️`Snowflake` -> [clone_raw_postgres_pipeline](https://about.gitlab.com/handbook/business-technology/data-team/platform/ci-jobs/#clone_raw_postgres_pipeline) CI job
* Run one of this pipeline. Depends on the file you changed, you should run either:
    * [ ] 🚂`Extract` -> [gitlab_saas_pgp_test](https://handbook.gitlab.com/handbook/business-technology/data-team/platform/ci-jobs/#-saas_pgp_test) if one of this file(s) is changed:
        * [ ] `el_saas_customers_scd_db_manifest.yaml`
        * [ ] `el_gitlab_dotcom_db_manifest.yaml`
        * [ ] `el_gitlab_dotcom_scd_db_manifest.yaml`
    * [ ] 🚂`Extract` -> [gitlab_ops_pgp_test](https://about.gitlab.com/handbook/business-technology/data-team/platform/ci-jobs/#gitlab_ops_pgp_test) if one of this file(s) is changed:
        * [ ] `el_saas_gitlab_ops_db_manifest.yaml`
        * [ ] `el_saas_gitlab_ops_scd_db_manifest.yaml`
* Enter the `database_type` of the modified table in the above manifests(ci, main, ops, customers)

It should pass properly to move forward.

##### Check downstream impact

Determine if there is downstream impact. I.e. additional changes to downstream models or the need for a manual full refresh in dbt.

- [ ] Downstream impact has been determined and;
   - [ ] changes to the manifest do not have downstream impact.
   - [ ] changes to the manifest have downstream impact and
      - [ ] is handled in this MR.
      - [ ] new MR and or issue is opened.

## Submitter Checklist

* [ ] Any >200M row table is **modified**? If no, continue. Else, follow the `deletes_exempt` guidelines as explained in [the handbook](https://gitlab.com/gitlab-com/content-sites/internal-handbook/-/blob/main/content/handbook/enterprise-data/platform/pipelines/_index.md?ref_type=heads#pgp-manifest-definition).
* [ ] Confirm there is no `RED` data loaded due to your changes. For more details about Data Classification refer to [Data Classification Standard](https://about.gitlab.com/handbook/security/data-classification-standard.html) page

## All MRs Checklist
- [ ] [Label hygiene](https://about.gitlab.com/handbook/business-ops/data-team/how-we-work/#issue-labeling) on issue.
    - [ ] Set workflow to `6 - review` (or type command `/label ~"workflow::6 - review" ` in the comment)
- [ ] Branch set to delete. (Leave option `Squash commits when merge request is accepted.` unchecked)
- [ ] This MR is ready for final review and merge.
- [ ] All threads are resolved.
- [ ] Remove the `Draft:` prefix in the MR title before assigning to reviewer (or type command `/ready` in the comment)
- [ ] Assigned to reviewer.

## Reviewer Checklist
- [ ]  Check before setting to merge

## Further changes requested
* [ ] **AUTHOR**: Uncheck all boxes before taking further action.
* [ ] If any of `huge` table is modified? If yes, please refer to the page [Large table backfilling](https://about.gitlab.com/handbook/business-technology/data-team/platform/pipelines/SAAS-Gitlab-com/#large-tables-backfilling) and follow the steps to deploy MR avoiding work days. Be sure to send out an outage notification in slack before-hand so that the business stakeholders are aware of the changes being made.

/label ~"postgres pipeline (pgp)" ~Python

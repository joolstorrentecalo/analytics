# Data Triage

<!--
Please complete all items. Ask questions in the #data slack channel
--->

# Housekeeping
* [ ] Title the issue as "<ISO date> Data Triage" e.g. **2020-07-09 Data Triage**
* [ ] Assign this issue to both the Data Analyst, Analytics Engineer and Data Engineer assigned to Triage
* [ ] [Add a weight to the issue](https://about.gitlab.com/handbook/business-ops/data-team/how-we-work/#issue-pointing)
* [ ] Link any issues opened as a result of Data Triage to this `parent` issue.
* [ ] Link the previous day triage issue to present day's triage issue.
## Data Analyst tasks
All tasks below should be checked off at the end of Triage day.

### Reply to slack channels
* [ ] Review each slack message request in the **#data** channel
    - [ ] Reply to slack threads by pointing GitLab team member to the appropriate handbook page or visualization.
    - [ ] Direct GitLab team member to the channel description, which has the link to the Data team project, if the request requires more than 5 minutes of investigative effort from a Data team member.
    - [ ] Ping [Triage Group](https://about.gitlab.com/handbook/business-technology/data-team/how-we-work/triage/#enterprise-data-program-triage-instructions) who may know more about the topic. You can also ping the Slack Alias for the relevant Triage Group.

### Friends and family days
* [ ] As we currently have a no-merge Friday rule if there is an upcoming family and friends day during your triage week which affects this please ensure this message (or similar) is shared #data channel by Tuesday at the latest:
* ```:awesome-dog-pug: :siren-siren:  Hi everyone, just a small FYI / reminder that due to the family and friends day this week the last day to merge MR’s in to the analytics repo this week will be Wednesday. :awesome-dog-pug: :siren-siren:```

## Analytics Engineer tasks

The focus area for the Analytics Engineer are the `DBT` models including the applied tests. The source for the tasks below are the Airflow logs posted in **#analytics-pipelines** and the Sisense Trusted Data Operations [Dashboard](https://app.periscopedata.com/app/gitlab/756199/TD:-Trusted-Data-Operations-Dashboard).

* [ ] Review each slack message in the **#data-triage** channel, which will inform the triager of what issues have been opened in the data team project that day.  Because this channel can sometimes be difficult to keep track of, you **should** also look at [issues with the ~"Needs Triage" label](https://gitlab.com/gitlab-data/analytics/-/issues?label_name%5B%5D=Needs+Triage&scope=all&state=opened), as this label is added every hour to issues that may have been missed.
    - [ ] For each issue opened by a non-Data Team member, label the issue by:
        - [ ] Adding the `Workflow::start (triage)` and `Triage` label
        - [ ] Adding additional [labels](https://about.gitlab.com/handbook/business-ops/data-team/how-we-work/#issue-labeling)
        - [ ] Assigning the issue based on:
            - [ ] the [CODEOWNERS file](https://gitlab.com/gitlab-data/analytics/blob/master/CODEOWNERS) for specific dbt model failures
            - [ ] the [functional DRIs](https://about.gitlab.com/handbook/business-ops/data-team/organization/#team-organization)
            - [ ] OR to the  Manager, Data if you aren't sure.

* [ ] [Create an issue](https://gitlab.com/gitlab-data/analytics/issues/new?issuable_template=Triage%3A%20Errors%20AE) for each new failed DBT model.
    * [ ] Link to all resulting issues and MRs in slack in **#analytics-pipelines**.
    * [ ] Use the AE issue template to resolve the dbt-run failure.
* [ ] [Create an issue](https://gitlab.com/gitlab-data/analytics/issues/new?issuable_template=Triage%3A%20Errors%20AE) for each new failed DBT test.
    * [ ] Link to all resulting issues and MRs in slack in **#analytics-pipelines**
    * [ ] Use the AE issue template to resolve the dbt-test failure.
* [ ] Review all dbt-test warnings and [create an issue](https://gitlab.com/gitlab-data/analytics/issues/new?issuable_template=Triage%3A%20Errors%20AE) if needed
* [ ] Investigate and respond to each **active** failure and alert in **#analytics-pipelines** by:
    * [ ] Updating the [Monte Carlo](https://getmontecarlo.com/monitors) status via Slack according to the defined action
    * [ ] [Creating an issue](https://gitlab.com/gitlab-data/analytics/issues/new?issuable_template=Triage%3A%20Errors%20AE) for each failure or relevant alert or reference the failure or alert in a related DBT Model Run failure issue. Use the AE issue template to resolve the Monte Carlo failure and alert.
    * [ ] [Opening an incident issue](https://gitlab.com/gitlab-data/analytics/-/issues/new?issuable_template=%5BReport%5D%20Incident%20Template&issue[issue_type]=incident) when the failure requires [immediate action](https://about.gitlab.com/handbook/business-technology/data-team/how-we-work/#incidents) in order to avoid or rememdy a data outage.
        * [ ] Link to all resulting incidents/issues and MRs in Slack

*Occasionally you we encounter connection errors, in which case [re-running the Airflow task](https://airflow.apache.org/docs/apache-airflow/1.10.15/dag-run.html#re-run-tasks) may be more appropriate than creating an issue. If the connection error persists then create an issue and escalate to the triage Data Engineer.*

* [ ] At the end of your working day post EOD message to slack along with a link to this issue in **#analytics-pipelines** and **#data-triage** so that it is clear for the next triager what time to check for issues from.

### Prepare for Next Milestone
* [ ] Groom Issues for Next Milestone: for issues that have missing or poor requirements, add comments in the issue asking questions to the Business DRI.
* [ ] Update the planning issues for this milestone and the next milestone
* [ ] Close/clean-up any irrelevant issues in your backlog.

<details>
<summary>instructions</summary>

```
dbt-test errors: <Link to airflow log>

Completed with x errors and x warnings:

##### Existing errors
| Issue | Error |
| ----- | ----- |

##### New errors
| Issue | Error |
| ----- | ----- |

##### Warnings
| Warning |
| ------- |
```

* Quick procedure to cleanup the log:
  1. Open any text editor with a regex find and replace; run through the below strings doing a find and replace for all:
        * `^(?!.*(Failure in test|Database error|Warning)).*$`
        * `^\[\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3}\] INFO - b'\\x1b\[0m`
        * `\\n'`
        * `^\R`
  2. In order, each of these lines:
     1. Removes all lines without Database Failure or Test Failure
     2. Removes date and INFO from each line
     3. Removes extra characters from the end of the string
     4. Removes empty lines

</details>


## Data Engineer tasks

The focus area for the Data Engineer are the Data Pipelines and Data Infrastructure.
* [ ] Investigate and respond to each **active** failure and alert in **#data-pipelines** and **#data-prom-alerts** by:
    * [ ] Updating the [Monte Carlo](https://getmontecarlo.com/monitors) status via Slack according to the defined action
    * [ ] [Creating an issue](https://gitlab.com/gitlab-data/analytics/issues/new?issuable_template=Triage%3A%20Errors%20DE) for each failure or relevant alert.
    * [ ] [Opening an incident issue](https://gitlab.com/gitlab-data/analytics/-/issues/new?issuable_template=%5BReport%5D%20Incident%20Template&issue[issue_type]=incident) when the failure requires [immediate action](https://about.gitlab.com/handbook/business-technology/data-team/how-we-work/#incidents) in order to avoid or rememdy a data outage.
       * [ ] Notify Data Customers of any [data refresh SLO](https://about.gitlab.com/handbook/business-technology/data-team/platform/#extract-and-load) breach by posting a message to the `#data` Slack channel using the appropriate Data Notification Template
* [ ] Investigate the `Data Warehouse::Impact Check` [MR list](https://gitlab.com/groups/gitlab-org/-/merge_requests?scope=all&state=all&label_name[]=Data%20Warehouse%3A%3AImpact%20Check&draft=no&approved_by_usernames[]=Any) for any schema changes to the gitlab/customers database. More detail in the [handbook](https://about.gitlab.com/handbook/business-technology/data-team/how-we-work/triage/#gitlabcom-databases-structure-changes).
* [ ] Check for any unresolved [alerts](https://getmontecarlo.com/alerts?statuses=no_status&is_normalized=false) (incidents with `No Status`) in Monte Carlo. **All such incidents should be given the appropriate status and have an open issue on the [DE - Triage Errors board](https://gitlab.com/groups/gitlab-data/-/boards/1917859)** end of day.

In addition to these tasks, the Data Engineer on triage should be focused on resolving these issues, including the backlog found on the [DE - Triage Errors board](https://gitlab.com/groups/gitlab-data/-/boards/1917859) as well as updating the [runbooks](https://gitlab.com/gitlab-data/runbooks) project where relevant.

* [ ] At the end of your working day post EOD message to slack along with a link to this issue only in **#data-pipelines**  so that it is clear demarcation that all the alert until this point has either open issue in [DE - Triage Errors board](https://gitlab.com/groups/gitlab-data/-/boards/1917859) or  issue has been resolved.

### Data Notification Templates

Use these to notify stakeholders of Data Delays.

<details>
<summary><i>Data Source Delay Templates</i></summary>

Post notices to #data and cross-post to #whats-happening-at-gitlab

#### GitLab.com

Follow the [runbook](https://gitlab.com/gitlab-data/runbooks/-/blob/main/Gitlab_dotcom/Gitlab_DB_recreation_failure.md) for the communication around GitLab.com data incidents.

#### Salesforce

Message: We have identified a delay in the `Salesforce` data refresh and this problem potentially impacts any Sales related KPIs or SiSense dashboards. We are actively working on a resolution and will provide an update once the KPIs and SiSense dashboards have been brought up-to-date.

The `Salesforce` data in the warehouse and downstream models is accurate as of YYYY-MM-DD HH:MM UTC (HH:MM PST).

The DRI for this incident is `@username`.

The link to the Data Team Incident issue is <link>

`CC @jakebielecki, @leework, @Jack, @mvilain, @gtmanalyticstriage, @product-analysts @datateam`

#### Zuora

Message: We have identified a delay in the `Zuora` data refresh and this problem potentially impacts any Financial KPIs or SiSense dashboards. We are actively working on a resolution and will provide an update once the KPIs and SiSense dashboards have been brought up-to-date.

The `Zuora` data in the warehouse and downstream models is accurate as of YYYY-MM-DD HH:MM UTC (HH:MM PST).

The DRI for this incident is `@username`.

The link to the Data Team Incident issue is <link>

`CC @jakebielecki, @leework, @Jack, @mvilain, @gtmanalyticstriage, @product-analysts @datateam`

#### General

We have identified a delay in the `DATA SOURCE` data refresh. We are actively working on a resolution and will provide an update once data has been brought up-to-date.

The `DATA SOURCE` data in the warehouse and downstream models is accurate as of YYYY-MM-DD HH:MM UTC (HH:MM PST).

The DRI for this incident is `@username`.

The link to the Data Team Incident issue is <link>

</details>


## Sensitive Data Nullified Columns

Several columns were nullified or removed entirely from Snowflake, due to work being done to remove all red / sensitive data from Snowflake.
You can find details on those columns in the [Sensitive Data Handbook entry](https://internal.gitlab.com/handbook/enterprise-data/platform/sensitive-data/) and use it as a quick check, whenever some clarification request comes over via Slack, where team members are wondering why some data is suddenly missing.

## Finishing the Day


/label ~"workflow::In dev" ~"Housekeeping" ~"Data Team" ~"Documentation" ~"Triage" ~"Priority::1-Ops"

/assign me

{% docs wk_rpt_event_namespace_plan_monthly %}

**Description:**

This model captures the last plan available from mart_event_valid for a namespace during a 
given calendar month. This is the same logic used to attribute the namespace's usage for PI 
reporting (ex: [`rpt_event_xmau_metric_monthly`](https://dbt.gitlabdata.com/#!/model/model.gitlab_snowflake.rpt_event_xmau_metric_monthly)).

**Data Grain:**
* event_calendar_month
* dim_ultimate_parent_namespace_id

**Intended Usage**

This model is intended to be JOINed to the fct_event lineage in order to determine how 
a namespace's usage will be attributed during monthly reporting. It can also be leveraged 
to track how many plans a namespace has during a calendar month.

**Filters & Business Logic in this Model:**

* This model inherits all filters and business logic from [`mart_event_valid`](https://dbt.gitlabdata.com/#!/model/model.gitlab_snowflake.mart_event_valid#description).
* The current month is excluded.

**Important Nuance:**

This model looks at the entire calendar month where the "official" PI reporting models only 
look at the last 28 days of the month. Please apply the filter `WHERE has_event_during_reporting_period = TRUE` 
in order to have this model tie out to the other reporting models. 

Example: Namespace xyz has events on July 1-2, 2022, but nothing for the remainder of the month. 
Namespace xyz will have a record in this model where `has_event_during_reporting_period = FALSE` 
because it did not have any events during the last 28 days of the month.  Namespace xyz's usage 
will _not_ count in the other reporting models (ex: [`rpt_event_xmau_metric_monthly`](https://dbt.gitlabdata.com/#!/model/model.gitlab_snowflake.rpt_event_xmau_metric_monthly)), 
so the `has_event_during_reporting_period` flag needs to be used in order for the models to tie out.

{% enddocs %}


{% docs wk_rpt_behavior_code_suggestion_outcome %}

**Description:** Reporting model to enable Code Suggestion analysis and reporting at the grain of one record per suggestion. This model uses Snowplow events and can be used to calculate metrics like Acceptance Rate. Read more about how the Code Suggestions events work [here](https://gitlab.com/gitlab-org/editor-extensions/gitlab-lsp/-/blob/main/docs/telemetry.md).

**Data Grain:** suggestion_id

This is an alias of `event_label` from the Snowplow data

**Filters Applied to Model:**

- Include events from the app_id `gitlab_ide_extension`
- Exclude events without an `event_label` (aka `suggestion_id`)
- Exclude suggestions that do not have a `suggestion_requested` event
- Exclude suggestions that have more than one event for a given `event_action`
- Exclude `suggestion_rejected` events if the suggestion also has a `suggestion_accepted` event (see "Other Comments" below)
- `Inherited` - Include events containing the `code_suggestions_context`
- `Inherited` - Exclude IDE events from VS Code extension version 3.76.0. These are excluded by using both `ide_name` and `extension_version` values.

**Intended Usage**

This model is intended to enable reporting and analysis on the "outcome" of a suggestion. It 
can be used to calculate Acceptance Rate, Load Time, etc.

**Other Comments:**

- A suggestion cannot be both accepted and rejected, but it can have both `suggestion_accepted` 
and `suggestion_rejected` events. The explanation is in [this issue comment](https://gitlab.com/gitlab-data/product-analytics/-/issues/1410#note_1581747408)
- A visual representation of the different Snowplow events associated with the single suggestion 
can be found [here](https://gitlab.com/gitlab-org/editor-extensions/gitlab-lsp/-/blob/main/docs/telemetry.md)

{% enddocs %}


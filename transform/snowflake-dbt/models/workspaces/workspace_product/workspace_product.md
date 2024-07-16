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

{% docs wk_rpt_gitlab_registered_users_monthly %}

**Description:**

This model captures the count of total, paid, and free users by month, delivery type, and deployment type.

**Data Grain:**

* reporting_month
* delivery_type
* deployment_type

**Filters Applied to this Model:**

* This model contains data going back to `2022-01-01`.
* The current month is excluded.
* Only paid seats from GitLab base products (i.e., normal tiers and not add-ons) are included in 
the paid user count. Removing this filter could lead to double-counting of users.
  * The exception is [Enterprise Agile Planning seats](https://docs.gitlab.com/ee/subscriptions/gitlab_com/#enterprise-agile-planning) (those _are_ included) since they are 
  incremental seats (and a base product license is not required to use them).
* Seats are limited to subscriptions with a status of `Active` or `Cancelled`.

**Business Logic in this Model:**

* `total_user_count` is defined using `instance_user_count` (aka [`active_user_count`](https://gitlab.com/gitlab-org/gitlab/-/blob/master/config/metrics/license/20210204124829_active_user_count.yml)) 
from the last ping of the month per installation. 
  * In this case "active" is referring to a user's state (ex. not blocked) as opposed to an indication of user activity with the product
* `paid_user_count` is defined using the count of paid seats for GitLab base products (i.e., not add-ons) for the given month
  * There are edge cases where `paid_user_count` is greater than `total_user_count` (ex: more Dedicated licenses were sold than there were registered Dedicated users). In this case, we set `paid_user_count` to equal `total_user_count`.
* `free_user_count` is defined as `total_user_count - paid_user_count`

**Callouts:**

* Even though Dedicated is a paid-only product, there are only free users in January-March 2022. 
This is because there were not yet active paid subscriptions for Dedicated.
* There are edge cases where `paid_user_count` is greater than `total_user_count` (ex: more Dedicated licenses were sold than there were registered Dedicated users). In this case, we set `paid_user_count` to equal `total_user_count`.

{% enddocs %}

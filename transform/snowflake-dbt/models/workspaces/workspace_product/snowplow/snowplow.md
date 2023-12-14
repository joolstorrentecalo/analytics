{% docs wk_mart_behavior_structured_event_code_suggestion %}

**Description:** Enriched Snowplow table for the analysis of Code Suggestions-related structured events. This model is limited to events carrying the `code_suggestions_context`, in addition to other filters (listed below). It enhances `fct_behavior_structured_event` and includes fields from the `code_suggestions_context` and `ide_extension_version` contexts.

**Data Grain:** behavior_structured_event_pk

This ID is generated using `event_id` from [prep_snowplow_unnested_events_all](https://dbt.gitlabdata.com/#!/model/model.gitlab_snowflake.prep_snowplow_unnested_events_all) 

**Filters Applied to Model:**
- Include events containing the `code_suggestions_context`
- Include events from the following app_ids: `gitlab_ai_gateway`, `gitlab_ide_extension`
- Exclude IDE events from VS Code extension version 3.76.0. These are excluded by using both `ide_name` and `extension_version` values.
  - Note: The Gateway did not send duplicate events from that extension version, so it is okay to let those flow through
- `Inherited` - This model only includes Structured events (when `event=struct` from `dim_behavior_event`)

**Tips for use:**
- There is a cluster key on `behavior_at::DATE`. Using `behavior_at` in a WHERE clause or INNER JOIN will improve query performance.
- All events will carry the `code_suggestions_context`, but only a subset will contain the `ide_extension_version` context
- `app_id = 'gitlab_ai_gateway'` 
  - These events originate from the AI gateway and cannot be blocked
  - There is only one event per suggestion (upon the request), which carries an `event_action` of `suggestions_requested` or `suggestion_requested`. Therefore these events can only be used to get a counts of users, etc, not acceptance rate.
  - These events do not carry the suggestion identifier in `event_label`
  - These events carry the `code_suggestions_context`, but not the `ide_extension_version` context
- `app_id = 'gitlab_ide_extension'`
  - These events originate from the IDEs, but can be blocked by the user (e.g. via disabling tracking)
  - There can be multiple events per suggestion, all with different `event_action` values (ex: `suggestion_requested`, `suggestion_loaded`, `suggestion_shown`, etc). Therefore these events can be used to calculate acceptance rate, etc.
  - These events carry a unique suggestion identifier in `event_label`. This can be joined across multiple events to calculate acceptance rate, etc.
  - These events carry both the `code_suggestions_context` and the `ide_extension_version` context

**Other Comments:**
- Schema for `code_suggestions_context` [here](https://gitlab.com/gitlab-org/iglu/-/tree/master/public/schemas/com.gitlab/code_suggestions_context)
- Schema for `ide_extension_version` context [here](https://gitlab.com/gitlab-org/iglu/-/tree/master/public/schemas/com.gitlab/ide_extension_version)
- A visual representation of the different Snowplow events coming from the IDE extension can be found [here](https://gitlab.com/gitlab-org/editor-extensions/gitlab-lsp/-/blob/main/docs/telemetry.md)
- Structured events are custom events implemented with five parameters: event_category, event_action, event_label, event_property and event_value. Snowplow documentation on [types of events](https://docs.snowplow.io/docs/understanding-tracking-design/out-of-the-box-vs-custom-events-and-entities/).
- There is information on some Snowplow structured events in the [Snowplow event dictionary](https://metrics.gitlab.com/snowplow)

{% enddocs %}

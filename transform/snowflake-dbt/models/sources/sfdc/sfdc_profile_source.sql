WITH source AS (
    
    SELECT * 
    FROM {{ source('salesforce', 'profile') }}

), renamed AS (

       SELECT

         --keys
          id	AS	profile_id,
      
         --info 
          description AS	profile_description,
          userlicenseid AS user_license_id,
          usertype AS	user_type,
          name AS	profile_name,
          createdbyid	AS	created_by_id,
          createddate	AS	created_date,

          --Indicator columns
          isssoenabled AS	is_sso_enabled,
          permissionsaccesscmc	AS	is_permissions_access_cmc,
          permissionsaccesscontentbuilder	AS	is_permissions_access_content_builder,
          permissionsaccountswitcheruser	AS	is_permissions_account_switcher_user,
          permissionsactivatecontract	AS	is_permissions_activate_contract,
          permissionsactivateorder	AS	is_permissions_activate_order,
          permissionsactivitiesaccess	AS	is_permissions_activities_access,
          permissionsadddirectmessagemembers	AS	is_permissions_add_direct_message_members,
          permissionsaicreateinsightobjects	AS	is_permissions_ai_create_insight_objects,
          permissionsaiviewinsightobjects	AS	is_permissions_ai_view_insight_objects,
          permissionsalloweribasicaccess	AS	is_permissions_allower_ibasic_access,
          permissionsallowobjectdetection	AS	is_permissions_allow_object_detection,
          permissionsallowobjectdetectiontraining	AS	is_permissions_allow_object_detection_training,
          permissionsallowuniversalsearch	AS	is_permissions_allow_universal_search,
          permissionsallowvieweditconvertedleads	AS	is_permissions_allow_viewed_it_converted_leads,
          permissionsallowviewknowledge	AS	is_permissions_allow_view_knowledge,
          permissionsapexrestservices	AS	is_permissions_apex_rest_services,
          permissionsapienabled	AS	is_permissions_api_enabled,
          permissionsapiuseronly	AS	is_permissions_api_user_only,
          permissionsassignpermissionsets	AS	is_permissions_assign_permission_sets,
          permissionsassigntopics	AS	is_permissions_assign_topics,
          permissionsauthorapex	AS	is_permissions_author_apex,
          permissionsautomateddataentry	AS	is_permissions_automated_data_entry,
          permissionsautomaticactivitycapture	AS	is_permissions_automatic_activity_capture,
          permissionsb2bmarketinganalyticsuser	AS	is_permissions_b2b_marketing_analytics_user,
          permissionsbotmanagebots	AS	is_permissions_bot_manage_bots,
          permissionsbotmanagebotstrainingdata	AS	is_permissions_bot_manage_bots_training_data,
          permissionsbulkapiharddelete	AS	is_permissions_bulk_api_hard_delete,
          permissionsbulkmacrosallowed	AS	is_permissions_bulk_macros_allowed,
          permissionscallcoachinguser	AS	is_permissions_call_coaching_user,
          permissionscampaigninfluence2	AS	is_permissions_campaign_influence2,
          permissionscanaccessce	AS	is_permissions_can_access_ce,
          permissionscanapprovefeedpost	AS	is_permissions_can_approve_feed_post,
          permissionscaneditprompts	AS	is_permissions_can_edit_prompts,
          permissionscaninsertfeedsystemfields	AS	is_permissions_can_insert_feed_system_fields,
          permissionscanrunanalysis	AS	is_permissions_can_run_analysis,
          permissionscanusenewdashboardbuilder	AS	is_permissions_can_use_new_dashboard_builder,
          permissionscanverifycomment	AS	is_permissions_can_verify_comment,
          permissionschangedashboardcolors	AS	is_permissions_change_dashboard_colors,
          permissionschattercomposeuicodesnippet	AS	is_permissions_chatter_compose_ui_code_snippet,
          permissionschattereditownpost	AS	is_permissions_chatter_edit_own_post,
          permissionschattereditownrecordpost	AS	is_permissions_chatter_edit_own_record_post,
          permissionschatterfilelink	AS	is_permissions_chatter_file_link,
          permissionschatterinternaluser	AS	is_permissions_chatter_internal_user,
          permissionschatterinviteexternalusers	AS	is_permissions_chatter_invite_external_users,
          permissionschatterowngroups	AS	is_permissions_chatter_own_groups,
          permissionscloseconversations	AS	is_permissions_close_conversations,
          permissionsconfigcustomrecs	AS	is_permissions_config_custom_recs,
          permissionsconnectorgtoenvironmenthub	AS	is_permissions_connect_org_to_environment_hub,
          permissionsconsentapiupdate	AS	is_permissions_consent_api_update,
          permissionscontentadministrator	AS	is_permissions_content_administrator,
          permissionscontenthubuser	AS	is_permissions_content_hub_user,
          permissionscontentworkspaces	AS	is_permissions_content_workspaces,
          permissionsconvertleads	AS	is_permissions_convert_leads,
          permissionscreateauditfields	AS	is_permissions_create_audit_fields,
          permissionscreatecustomizedashboards	AS	is_permissions_create_customize_dashboards,
          permissionscreatecustomizefilters	AS	is_permissions_create_customize_filters,
          permissionscreatecustomizereports	AS	is_permissions_create_customize_reports,
          permissionscreatedashboardfolders	AS	is_permissions_create_dashboard_folders,
          permissionscreateltngtempfolder	AS	is_permissions_create_ltng_temp_folder,
          permissionscreateltngtempinpub	AS	is_permissions_create_ltng_temp_in_pub,
          permissionscreatemultiforce	AS	is_permissions_create_multiforce,
          permissionscreatereportfolders	AS	is_permissions_create_report_folders,
          permissionscreatereportinlightning	AS	is_permissions_create_reportin_lightning,
          permissionscreatetopics	AS	is_permissions_create_topics,
          permissionscreateworkbadgedefinition	AS	is_permissions_create_workbadge_definition,
          permissionscreateworkspaces	AS	is_permissions_create_workspaces,
          permissionscustomizeapplication	AS	is_permissions_customize_application,
          permissionscustommobileappsaccess	AS	is_permissions_custom_mobile_apps_access,
          permissionsdataexport	AS	is_permissions_data_export,
          permissionsdebugapex	AS	is_permissions_debug_apex,
          permissionsdelegatedportaluseradmin	AS	is_permissions_delegated_portal_user_admin,
          permissionsdelegatedtwofactor	AS	is_permissions_delegated_two_factor,
          permissionsdeleteactivatedcontract	AS	is_permissions_delete_activated_contract,
          permissionsdeletetopics	AS	is_permissions_delete_topics,
          permissionsdistributefromperswksp	AS	is_permissions_distribute_from_perswksp,
          permissionseditactivatedorders	AS	is_permissions_edit_activated_orders,
          permissionseditbillinginfo	AS	is_permissions_edit_billing_info,
          permissionseditbrandtemplates	AS	is_permissions_edit_brand_templates,
          permissionseditcasecomments	AS	is_permissions_edit_case_comments,
          permissionseditevent	AS	is_permissions_edit_event,
          permissionsedithtmltemplates	AS	is_permissions_edit_html_templates,
          permissionseditknowledge	AS	is_permissions_edit_knowledge,
          permissionseditmydashboards	AS	is_permissions_edit_my_dashboards,
          permissionseditmyreports	AS	is_permissions_edit_my_reports,
          permissionseditopplineitemunitprice	AS	is_permissions_edit_opp_lineitem_unit_price,
          permissionseditpublicdocuments	AS	is_permissions_edit_public_documents,
          permissionseditpublicfilters	AS	is_permissions_edit_public_filters,
          permissionseditpublictemplates	AS	is_permissions_edit_public_templates,
          permissionseditreadonlyfields	AS	is_permissions_edit_read_only_fields,
          permissionsedittask	AS	is_permissions_edit_task,
          permissionsedittopics	AS	is_permissions_edit_topics,
          permissionsemailadministration	AS	is_permissions_email_administration,
          permissionsemailmass	AS	is_permissions_email_mass,
          permissionsemailsingle	AS	is_permissions_email_single,
          permissionsemailtemplatemanagement	AS	is_permissions_email_template_management,
          permissionsemployeeexperience	AS	is_permissions_employee_experience,
          permissionsenablecommunityapplauncher	AS	is_permissions_enable_community_app_launcher,
          permissionsenablenotifications	AS	is_permissions_enable_notifications,
          permissionsexportreport	AS	is_permissions_export_report,
          permissionsfeedpinning	AS	is_permissions_feed_pinning,
          permissionsflowuflrequired	AS	is_permissions_flow_ufl_required,
          permissionsforcetwofactor	AS	is_permissions_force_two_factor,
          permissionsfsccomprehensiveuseraccess	AS	is_permissions_fsc_comprehensive_user_access,
          permissionsgiverecognitionbadge	AS	is_permissions_give_recognition_badge,
          permissionsgovernnetworks	AS	is_permissions_govern_networks,
          permissionshasunlimitederbscoringrequests	AS	is_permissions_has_unlimited_erb_scoring_requests,
          permissionshasunlimitednbaexecutions	AS	is_permissions_has_unlimited_nba_executions,
          permissionsheadlesscmsaccess	AS	is_permissions_headless_cms_access,
          permissionshidereadbylist	AS	is_permissions_hide_read_by_list,
          permissionsidentityconnect	AS	is_permissions_identity_connect,
          permissionsidentityenabled	AS	is_permissions_identity_enabled,
          permissionsimportcustomobjects	AS	is_permissions_import_custom_objects,
          permissionsimportleads	AS	is_permissions_import_leads,
          permissionsimportpersonal	AS	is_permissions_import_personal,
          permissionsinboundmigrationtoolsuser	AS	is_permissions_inbound_migration_tools_user,
          permissionsinstallmultiforce	AS	is_permissions_install_multiforce,
          permissionsisotopeaccess	AS	is_permissions_isotope_access,
          permissionsisotopectocuser	AS	is_permissions_isotope_ctoc_user,
          permissionsisotopelex	AS	is_permissions_isotopelex,
          permissionsisssoenabled	AS	is_permissions_is_sso_enabled,
          permissionsleadscoreresultpublisher	AS	is_permissions_lead_score_result_publisher,
          permissionsleadscoreuser	AS	is_permissions_lead_score_user,
          permissionslearningmanager	AS	is_permissions_learning_manager,
          permissionslightningconsoleallowedforuser	AS	is_permissions_lightning_console_allowed_for_user,
          permissionslightningexperienceuser	AS	is_permissions_lightning_experience_user,
          permissionslistemailsend	AS	is_permissions_list_email_send,
          permissionslmendmessagingsessionuserperm	AS	is_permissions_lmend_messaging_session_user_perm,
          permissionslmoutboundmessaginguserperm	AS	is_permissions_lmout_bound_messaging_user_perm,
          permissionsltngpromoreserved01userperm	AS	is_permissions_ltng_promo_reserved_01_user_perm,
          permissionsmanageanalyticsnapshots	AS	is_permissions_manage_analytic_snapshots,
          permissionsmanageauthproviders	AS	is_permissions_manage_auth_providers,
          permissionsmanagebusinesshourholidays	AS	is_permissions_manage_business_hour_holidays,
          permissionsmanagec360aconnections	AS	is_permissions_manage_c360_aconnections,
          permissionsmanagecallcenters	AS	is_permissions_manage_call_centers,
          permissionsmanagecases	AS	is_permissions_manage_cases,
          permissionsmanagecategories	AS	is_permissions_manage_categories,
          permissionsmanagecertificates	AS	is_permissions_manage_certificates,
          permissionsmanagechattermessages	AS	is_permissions_manage_chatter_messages,
          permissionsmanagecms	AS	is_permissions_manage_cms,
          permissionsmanagecontentpermissions	AS	is_permissions_manage_content_permissions,
          permissionsmanagecontentproperties	AS	is_permissions_manage_content_properties,
          permissionsmanagecontenttypes	AS	is_permissions_manage_content_types,
          permissionsmanagecustompermissions	AS	is_permissions_manage_custom_permissions,
          permissionsmanagecustomreporttypes	AS	is_permissions_manage_custom_report_types,
          permissionsmanagedashbdsinpubfolders	AS	is_permissions_manage_dashbds_in_pub_folders,
          permissionsmanagedatacategories	AS	is_permissions_manage_data_categories,
          permissionsmanagedataintegrations	AS	is_permissions_manage_data_integrations,
          permissionsmanagedynamicdashboards	AS	is_permissions_manage_dynamic_dashboards,
          permissionsmanageemailclientconfig	AS	is_permissions_manage_email_client_config,
          permissionsmanageencryptionkeys	AS	is_permissions_manage_encryption_keys,
          permissionsmanageexchangeconfig	AS	is_permissions_manage_exchange_config,
          permissionsmanageexternalconnections	AS	is_permissions_manage_external_connections,
          permissionsmanagehealthcheck	AS	is_permissions_manage_health_check,
          permissionsmanagehubconnections	AS	is_permissions_manage_hub_connections,
          permissionsmanageinteraction	AS	is_permissions_manage_interaction,
          permissionsmanageinternalusers	AS	is_permissions_manage_internal_users,
          permissionsmanageipaddresses	AS	is_permissions_manage_ip_addresses,
          permissionsmanageknowledge	AS	is_permissions_manage_knowledge,
          permissionsmanageknowledgeimportexport	AS	is_permissions_manage_knowledge_import_export,
          permissionsmanageleads	AS	is_permissions_manage_leads,
          permissionsmanagelearningreporting	AS	is_permissions_manage_learning_reporting,
          permissionsmanageloginaccesspolicies	AS	is_permissions_manage_log_inaccess_policies,
          permissionsmanagemobile	AS	is_permissions_manage_mobile,
          permissionsmanagenetworks	AS	is_permissions_manage_networks,
          permissionsmanagepartners	AS	is_permissions_manage_partners,
          permissionsmanagepasswordpolicies	AS	is_permissions_manage_password_policies,
          permissionsmanageprofilespermissionsets	AS	is_permissions_manage_profiles_permission_sets,
          permissionsmanagepropositions	AS	is_permissions_manage_propositions,
          permissionsmanagepvtrptsanddashbds	AS	is_permissions_manage_pvt_rpts_and_dashbds,
          permissionsmanagequotas	AS	permissions_manage_quotas,
          permissionsmanagerecommendationstrategies	AS	is_permissions_manage_recommendation_strategies,
          permissionsmanagereleaseupdates	AS	is_permissions_manage_release_updates,
          permissionsmanageremoteaccess	AS	is_permissions_manage_remote_access,
          permissionsmanagereportsinpubfolders	AS	is_permissions_manage_reports_in_pub_folders,
          permissionsmanageroles	AS	is_permissions_manage_roles,
          permissionsmanagesandboxes	AS	is_permissions_manage_sandboxes,
          permissionsmanagesearchpromotionrules	AS	is_permissions_manage_search_promotion_rules,
          permissionsmanagesessionpermissionsets	AS	is_permissions_manage_session_permission_sets,
          permissionsmanagesharing	AS	is_permissions_manage_sharing,
          permissionsmanagesolutions	AS	is_permissions_manage_solutions,
          permissionsmanagesubscriptions	AS	is_permissions_manage_subscriptions,
          permissionsmanagesurveys	AS	is_permissions_manage_surveys,
          permissionsmanagesynonyms	AS	is_permissions_manage_synonyms,
          permissionsmanageterritories	AS	is_permissions_manage_territories,
          permissionsmanagetrustmeasures	AS	is_permissions_manage_trust_measures,
          permissionsmanagetwofactor	AS	is_permissions_manage_two_factor,
          permissionsmanageunlistedgroups	AS	is_permissions_manage_unlisted_groups,
          permissionsmanageusers	AS	is_permissions_manage_users,
          permissionsmassinlineedit	AS	is_permissions_mass_inline_edit,
          permissionsmergetopics	AS	is_permissions_merge_topics,
          permissionsmoderatechatter	AS	is_permissions_moderate_chatter,
          permissionsmoderatenetworkfeeds	AS	is_permissions_moderate_network_feeds,
          permissionsmoderatenetworkfiles	AS	is_permissions_moderate_network_files,
          permissionsmoderatenetworkmessages	AS	is_permissions_moderate_network_messages,
          permissionsmoderatenetworkusers	AS	is_permissions_moderate_network_users,
          permissionsmodifyalldata	AS	is_permissions_modify_all_data,
          permissionsmodifydataclassification	AS	is_permissions_modify_data_classification,
          permissionsmodifymetadata	AS	is_permissions_modify_metadata,
          permissionsnativewebviewscrolling	AS	is_permissions_native_webview_scrolling,
          permissionsnewreportbuilder	AS	is_permissions_new_report_builder,
          permissionsopportunityscoreuser	AS	is_permissions_opportunity_score_user,
          permissionsoutboundmigrationtoolsuser	AS	is_permissions_outbound_migration_tools_user,
          permissionsoverrideforecasts	AS	is_permissions_override_forecasts,
          permissionspackaging2	AS	is_permissions_packaging2,
          permissionspackaging2delete	AS	is_permissions_packaging2_delete,
          permissionspasswordneverexpires	AS	is_permissions_password_never_expires,
          permissionspreventclassicexperience	AS	is_permissions_prevent_classic_experience,
          permissionsprivacydataaccess	AS	is_permissions_privacy_data_access,
          permissionspublishmultiforce	AS	is_permissions_publish_multiforce,
          permissionsqueryallfiles	AS	is_permissions_query_all_files,
          permissionsquipmetricsaccess	AS	is_permissions_quip_metrics_access,
          permissionsquipuserengagementmetrics	AS	is_permissions_quip_user_engagement_metrics,
          permissionsrecordvisibilityapi	AS	is_permissions_record_visibility_api,
          permissionsremovedirectmessagemembers	AS	is_permissions_remove_direct_message_members,
          permissionsresetpasswords	AS	is_permissions_reset_passwords,
          permissionsrunflow	AS	is_permissions_run_flow,
          permissionsrunreports	AS	is_permissions_run_reports,
          permissionssalesconsole	AS	is_permissions_sales_console,
          permissionssalesforceiqinbox	AS	is_permissions_salesforce_iq_inbox,
          permissionssalesforceiqinternal	AS	is_permissions_salesforce_iq_internal,
          permissionssalesforcemeetingsuserperm	AS	is_permissions_salesforce_meetings_user_perm,
          permissionssandboxtestingincommunityapp	AS	is_permissions_sandbox_testing_incommunity_app,
          permissionssceviewalldata	AS	is_permissions_sce_view_all_data,
          permissionsschedulejob	AS	is_permissions_schedule_job,
          permissionsschedulereports	AS	is_permissions_schedule_reports,
          permissionsselectfilesfromsalesforce	AS	is_permissions_select_files_from_salesforce,
          permissionssendannouncementemails	AS	is_permissions_send_announcement_emails,
          permissionssendcustomnotifications	AS	is_permissions_send_custom_notifications,
          permissionssendsitrequests	AS	is_permissions_send_sit_requests,
          permissionssharefileswithnetworks	AS	is_permissions_share_files_with_networks,
          permissionsshareinternalarticles	AS	is_permissions_share_internal_articles,
          permissionsshowcompanynameasuserbadge	AS	is_permissions_show_company_name_as_user_badge,
          permissionsskipidentityconfirmation	AS	is_permissions_skip_identity_confirmation,
          permissionssolutionimport	AS	is_permissions_solution_import,
          permissionsstdautomaticactivitycapture	AS	is_permissions_std_automatic_activity_capture,
          permissionssubmitmacrosallowed	AS	is_permissions_submit_macros_allowed,
          permissionssubscribedashboardrolesgrps	AS	is_permissions_subscribe_dashboard_roles_grps,
          permissionssubscribedashboardtootherusers	AS	is_permissions_subscribe_dashboard_toother_users,
          permissionssubscribereportrolesgrps	AS	is_permissions_subscribe_report_roles_grps,
          permissionssubscribereportsrunasuser	AS	is_permissions_subscribe_reports_run_as_user,
          permissionssubscribereporttootherusers	AS	is_permissions_subscribe_report_to_other_users,
          permissionssubscribetolightningdashboards	AS	is_permissions_subscribe_to_lightning_dashboards,
          permissionssubscribetolightningreports	AS	is_permissions_subscribe_to_lightning_reports,
          permissionstracexdsqueries	AS	is_permissions_trace_xds_queries,
          permissionstransactionalemailsend	AS	is_permissions_transactional_email_send,
          permissionstransferanycase	AS	is_permissions_transfer_any_case,
          permissionstransferanyentity	AS	is_permissions_transfer_any_entity,
          permissionstransferanylead	AS	is_permissions_transfer_any_lead,
          permissionstwofactorapi	AS	is_permissions_two_factor_api,
          permissionsupdatewithinactiveowner	AS	is_permissions_update_with_inactive_owner,
          permissionsuseassistantdialog	AS	is_permissions_use_assistant_dialog,
          permissionsusemysearch	AS	is_permissions_use_my_search,
          permissionsusequerysuggestions	AS	is_permissions_use_query_suggestions,
          permissionsuseteamreassignwizards	AS	is_permissions_use_team_reassign_wizards,
          permissionsuseweblink	AS	is_permissions_use_weblink,
          permissionsvideoconferencezoomuser	AS	is_permissions_video_conference_zoom_user,
          permissionsviewallactivities	AS	is_permissions_view_all_activities,
          permissionsviewallcustomsettings	AS	is_permissions_view_all_custom_settings,
          permissionsviewalldata	AS	is_permissions_view_all_data,
          permissionsviewallforecasts	AS	is_permissions_view_all_forecasts,
          permissionsviewallforeignkeynames	AS	is_permissions_view_all_foreign_key_names,
          permissionsviewallprofiles	AS	is_permissions_view_all_profiles,
          permissionsviewallusers	AS	is_permissions_view_all_users,
          permissionsviewanomalyevents	AS	is_permissions_view_anomaly_events,
          permissionsviewcaseinteraction	AS	is_permissions_view_case_interaction,
          permissionsviewcontent	AS	is_permissions_view_content,
          permissionsviewcustomersentiment	AS	is_permissions_view_customer_sentiment,
          permissionsviewdataassessment	AS	is_permissions_view_data_assessment,
          permissionsviewdatacategories	AS	is_permissions_view_data_categories,
          permissionsviewdataleakageevents	AS	is_permissions_view_data_leakage_events,
          permissionsviewencrypteddata	AS	is_permissions_view_encrypted_data,
          permissionsvieweventlogfiles	AS	is_permissions_view_event_log_files,
          permissionsviewforecastingpredictive	AS	is_permissions_view_forecasting_predictive,
          permissionsviewglobalheader	AS	is_permissions_view_global_header,
          permissionsviewhealthcheck	AS	is_permissions_view_health_check,
          permissionsviewhelplink	AS	is_permissions_view_help_link,
          permissionsviewmyteamsdashboards	AS	is_permissions_view_my_teams_dashboards,
          permissionsviewonlyembeddedappuser	AS	is_permissions_view_only_embedded_appuser,
          permissionsviewplatformevents	AS	is_permissions_view_platform_events,
          permissionsviewprivatestaticresources	AS	is_permissions_view_private_static_resources,
          permissionsviewpublicdashboards	AS	is_permissions_view_public_dashboards,
          permissionsviewpublicreports	AS	is_permissions_view_public_reports,
          permissionsviewrestrictionandscopingrules	AS	is_permissions_view_restriction_and_scoping_rules,
          permissionsviewroles	AS	is_permissions_view_roles,
          permissionsviewsetup	AS	is_permissions_view_setup,
          permissionsviewtrustmeasures	AS	is_permissions_view_trust_measures,
          permissionsviewuserpii	AS	is_permissions_view_user_pii,
          lastmodifiedbyid	AS	last_modified_by_id,
          lastmodifieddate	AS	last_modified_date,
          lastreferenceddate	AS	last_referenced_date,
          lastvieweddate	AS	last_viewed_date,

         --Stitch metadata
         systemmodstamp AS system_mod_stamp,
         _sdc_batched_at AS sdc_batched_at,
         _sdc_extracted_at AS sdc_extracted_at,
         _sdc_received_at AS sdc_received_at,
         _sdc_sequence AS sdc_sequence,
         _sdc_table_version AS sdc_table_version
      
       FROM source
)


SELECT *
FROM renamed


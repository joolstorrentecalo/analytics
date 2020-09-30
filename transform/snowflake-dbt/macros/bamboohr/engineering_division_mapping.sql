{% macro engineering_division_mapping(sub_department,jobtitle_speciality,technology_group) %}

CASE
    WHEN sub_department = 'ops' AND jobtitle_speciality = 'package' THEN 'package'
    WHEN sub_department = 'ops' AND jobtitle_speciality IN ('progressive delivery','release management') AND technology_group = 'backend' THEN 'release backend'
    WHEN sub_department = 'ops' AND jobtitle_speciality IN ('continuous integration','testing','health') AND technology_group = 'backend' THEN 'verify backend'
    WHEN sub_department = 'ops' AND ((jobtitle_speciality IN ('continuous integration','testing','verify','release','release management','progressive delivery') AND technology_group = 'frontend') or jobtitle_speciality = 'runner') THEN 'verify & release frontend'
    WHEN sub_department = 'ops' AND jobtitle_speciality = 'configure' THEN 'configure'
    WHEN sub_department = 'ops' AND ((jobtitle_speciality IN ('apm','health') AND technology_group IS NULL) OR jobtitle_speciality = 'monitor') THEN 'monitor stage'
    WHEN sub_department = 'ops' AND jobtitle_speciality = 'apm' AND technology_group IN ('backend','frontend') THEN 'monitor:apm'
    WHEN sub_department = 'ops' AND jobtitle_speciality = 'health' AND technology_group IN ('backend','frontend') THEN 'monitor:health'
    WHEN sub_department = 'dev' AND jobtitle_speciality = 'editor' AND technology_group = 'backend' THEN 'create:editor backend'
    WHEN sub_department = 'dev' AND jobtitle_speciality = 'editor' AND technology_group = 'frontend' THEN 'create:editor frontend'
    WHEN sub_department = 'dev' AND ((jobtitle_speciality = 'knowledge' AND technology_group = 'backend') or jobtitle_speciality = 'create') THEN 'create:knowledge backend'
    WHEN sub_department = 'dev' AND jobtitle_speciality IN ('knowledge','knowledge & editor') AND technology_group = 'frontend' THEN 'create:knowledge frontend'
    WHEN sub_department = 'dev' AND jobtitle_speciality = 'source code' AND technology_group = 'backend' THEN 'create:source code backend'
    WHEN sub_department = 'dev' AND jobtitle_speciality = 'source code' AND technology_group = 'frontend' THEN 'create:source code frontend'
    WHEN sub_department = 'dev' AND jobtitle_speciality = 'static site editor' THEN 'create:static site editor'
    WHEN sub_department = 'dev' AND jobtitle_speciality = 'gitaly' THEN 'gitaly'
    WHEN sub_department = 'dev' AND jobtitle_speciality = 'gitter' AND technology_group = 'backend' THEN 'gitter'
    WHEN sub_department = 'dev' AND jobtitle_speciality IN ('access','access & import','analytics','analytics & compliance','compliance') AND technology_group = 'backend' THEN 'manage backend'
    WHEN sub_department = 'dev' AND jobtitle_speciality IN ('access','access & compliance & import','analytics','compliance','import') AND technology_group = 'frontend' THEN 'manage & fulfillment frontend'
    WHEN sub_department = 'dev' AND jobtitle_speciality IN ('project management') AND technology_group = 'backend' THEN 'plan:project management backend'
    WHEN sub_department = 'dev' AND jobtitle_speciality IN ('portfolio management & certify') AND technology_group = 'backend' THEN 'plan:portfolio management backend'
    WHEN sub_department = 'dev' AND ((jobtitle_speciality IN ('portfolio management & certify','project management') AND technology_group = 'frontend') OR jobtitle_speciality = 'plan') THEN 'plan frontend'
    WHEN sub_department = 'dev' AND jobtitle_speciality = 'ecosystem' THEN 'ecosystem'
    WHEN sub_department = 'enablement' AND jobtitle_speciality = 'database' THEN 'database'
    WHEN sub_department = 'enablement' AND jobtitle_speciality = 'distribution' THEN 'distribution'
    WHEN sub_department = 'enablement' AND jobtitle_speciality = 'database' THEN 'database'
    WHEN sub_department = 'enablement' AND jobtitle_speciality = 'geo' THEN 'geo'
    WHEN sub_department = 'enablement' AND jobtitle_speciality IN ('memory','memory & database') THEN 'memory'
    WHEN sub_department = 'enablement' AND jobtitle_speciality IN ('global search') THEN 'search'
    WHEN sub_department = 'growth' AND jobtitle_speciality IN ('acquisition','acquisition & conversion & telemetry') THEN 'acquisition'
    WHEN sub_department = 'growth' AND jobtitle_speciality IN ('conversion') THEN 'conversion'
    WHEN sub_department = 'growth' AND jobtitle_speciality IN ('expansion','expansion & retention') THEN 'expansion'
    WHEN sub_department = 'growth' AND jobtitle_speciality IN ('retention') THEN 'retention'
    WHEN sub_department = 'growth' AND jobtitle_speciality IN ('fulfillment') AND technology_group = 'backend' THEN 'fulfillment backend'
    WHEN sub_department = 'growth' AND jobtitle_speciality IN ('fulfillment') AND technology_group = 'frontend' THEN 'fulfillment frontend'
    WHEN sub_department = 'growth' AND jobtitle_speciality IN ('telemetry') AND technology_group = 'backend' THEN 'telemetry backend'
    WHEN sub_department = 'growth' AND jobtitle_speciality IN ('telemetry') AND technology_group = 'frontend' THEN 'telemetry frontend'
    WHEN sub_department = 'secure' AND jobtitle_speciality IN ('composition analysis','dynamic analysis','dynamic analysis & fuzz testing','fuzz testing','secure','secure & defend','static analysis','vulnerability research') AND technology_group = 'backend' THEN 'secure backend'
    WHEN sub_department = 'secure' AND jobtitle_speciality IN ('secure') AND technology_group = 'frontend' THEN 'secure frontend'
    WHEN sub_department = 'threat management' THEN 'threat management'
    ELSE NULL END AS team_name

 {% endmacro %}

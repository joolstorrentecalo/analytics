-- unused currently
-- connection uses SECURITYADMIN role and ADMIN warehouse
set username = (select upper(:username));

set prep_db = (select $username || '_PREP');
set prod_db = (select $username || '_PROD');

DROP USER IF EXISTS identifier($username);
DROP ROLE IF EXISTS identifier($username);
DROP DATABASE IF EXISTS identifier($prep_db) CASCADE;
DROP DATABASE IF EXISTS identifier($prod_db) CASCADE;

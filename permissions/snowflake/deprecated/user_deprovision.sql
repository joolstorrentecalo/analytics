-- using the SECURITYADMIN role run the following by replacing 'USER_NAME' provided in Offboarding or Deprovisioning issue request.

-- set username = 'USER_NAME';

set prep_db = (select $username || '_PREP');
set prod_db = (select $username || '_PROD');

ALTER USER IF EXISTS identifier($username) SET DISABLED = TRUE;
DROP DATABASE IF EXISTS identifier($prep_db) CASCADE;
DROP DATABASE IF EXISTS identifier($prod_db) CASCADE;
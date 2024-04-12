-- IF GOING TO BE A DBT USER, run this to create the development databases
-- assumes sysadmin role, admin warehouse is being used
set username = (select upper(:username));
set prod_db = (select $username || '_PROD');
set prep_db = (select $username || '_PREP');

CREATE DATABASE identifier($prod_db);
GRANT OWNERSHIP ON DATABASE identifier($prod_db) to role identifier($username);
GRANT ALL PRIVILEGES ON DATABASE identifier($prod_db) to role identifier($username);

CREATE DATABASE identifier($prep_db);
GRANT OWNERSHIP ON DATABASE identifier($prep_db) to role identifier($username);
GRANT ALL PRIVILEGES ON DATABASE identifier($prep_db) to role identifier($username);

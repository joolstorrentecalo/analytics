-- assumes securityadmin role and admin warehouse are being used
SET username = (select upper(:username));
SET email = (select lower(:email));

CREATE USER identifier($username)
  LOGIN_NAME = $email
  DISPLAY_NAME = $username
  EMAIL = $email
  DEFAULT_WAREHOUSE = 'DEV_XS';

CREATE ROLE identifier($username);

GRANT ROLE identifier($username) TO ROLE "SYSADMIN";
GRANT ROLE identifier($username) TO USER identifier($username);

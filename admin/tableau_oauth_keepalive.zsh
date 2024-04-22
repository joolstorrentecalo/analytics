## Assumes tableau is installed. Will add CLIENT_SESSION_KEEP_ALIVE = True 
## to /opt/snowflake/snowflake-odbc/Setup/odbc.ini to prevent frequent OAUTH
## refresh prompt

ODBC_INI_FILE="/opt/snowflake/snowflakeodbc/Setup/odbc.ini"
KEEP_ALIVE_LINE="CLIENT_SESSION_KEEP_ALIVE=True"

echo "Checking for configuration file at: $ODBC_INI_FILE"
# Ensure file exists
if [[ ! -f "$ODBC_INI_FILE" ]]; then
  echo "Configuration file not found at: $ODBC_INI_FILE"
  exit 1
else 
  echo "Configuration file found at: $ODBC_INI_FILE"
fi


# Check if the line exists, if not, add it under the correct section
if ! grep -q "^CLIENT_SESSION_KEEP_ALIVE" $ODBC_INI_FILE; then
  # Insert the line under the [SnowflakeDSII] section, followed by a newline
  sudo sed -i.bak "/^\[SnowflakeDSII\]/a \\
$KEEP_ALIVE_LINE\\
" $ODBC_INI_FILE 

  echo "CLIENT_SESSION_KEEP_ALIVE setting added to $ODBC_INI_FILE"
else
  echo "CLIENT_SESSION_KEEP_ALIVE setting already exists in $ODBC_INI_FILE"
fi 

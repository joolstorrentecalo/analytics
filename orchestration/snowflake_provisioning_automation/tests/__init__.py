"""
Tweak path as due to script execution way in Airflow,
can't touch the original code
"""

import sys
import os

abs_path = os.path.dirname(os.path.realpath(__file__))
parent_path = abs_path[: abs_path.find("/tests")]
# sys.path.append(parent_path)

provision_users_path = os.path.join(parent_path, "provision_users/")
sys.path.append(provision_users_path)

update_roles_yaml_path = os.path.join(parent_path, "update_roles_yaml/")
sys.path.append(update_roles_yaml_path)

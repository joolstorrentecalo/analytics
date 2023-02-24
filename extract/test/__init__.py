"""
Tweak path as due to script execution way in Airflow,
can't touch the original code
"""
import os
import sys

abs_path = os.path.dirname(os.path.realpath(__file__))
abs_path_usage_ping = abs_path[: abs_path.find("extract")] + "extract/saas_usage_ping"
sys.path.append(abs_path_usage_ping)

abs_path_level_up = (
    abs_path[: abs_path.find("extract")] + "extract/level_up_thought_industries/src"
)
sys.path.append(abs_path_level_up)

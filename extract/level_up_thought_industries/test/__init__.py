"""
Tweak path as due to script execution way in Airflow,
can't touch the original code
"""

import sys
import os

abs_path = os.path.dirname(os.path.realpath(__file__))
level_up_source_path = abs_path[: abs_path.find("/test")] + "/src/"
sys.path.append(level_up_source_path)

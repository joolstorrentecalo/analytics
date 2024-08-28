"""
Tweak test_path as due to script execution way in Airflow,
can't touch the original code
"""

import os
import sys

TEST_PATH = "extract/hackerone/src"
absolute_test_path = os.path.dirname(os.path.realpath(__file__))
testing_full_path = absolute_test_path[: absolute_test_path.find("extract")] + TEST_PATH
sys.path.append(testing_full_path)

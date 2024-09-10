"""
Tweak test_path as due to script execution way in Airflow,
can't touch the original code
"""

import os
import sys

test_paths = [
    "extract/saas_usage_ping",
    "extract/adaptive/src",
    "extract/data_classification",
]
absolute_test_path = os.path.dirname(os.path.realpath(__file__))

for test_path in test_paths:
    testing_full_path = (
        absolute_test_path[: absolute_test_path.find("extract")] + test_path
    )
    sys.path.append(testing_full_path)

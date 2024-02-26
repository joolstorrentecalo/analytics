"""
Tweak path as due to script execution way in Airflow,
can't touch the original code
"""
import sys
import os

abs_path = os.path.dirname(os.path.realpath(__file__))
manifest_path = abs_path[: abs_path.find("/test")] + "/manifests/"
postgres_pipeline_path = abs_path[: abs_path.find("/test")] + "/postgres_pipeline/"
sys.path.append(abs_path)
sys.path.append(postgres_pipeline_path)

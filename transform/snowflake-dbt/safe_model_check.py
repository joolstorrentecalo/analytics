import json
import logging
import pandas as pd
from pandas import json_normalize


def check_safe_models(file):
    with open(file) as json_file:
        first_char = json_file.read()
        if not first_char:
            logging.info("All models are safe 🥂")
        else:
            df = json_normalize(pd.Series(open(file).readlines()).apply(json.loads))
            df = df[["name", "tags", "config.schema"]]
            error_message = "⚠️ The following models are not SAFE ⚠️:\r\n" + df.to_csv(
                index=False
            )
            raise ValueError(error_message)


def clean_up_json(file):
    with open(file, "r") as file:
        lines = file.readlines()
    valid_json_objects = []

    for line in lines:
        try:
            json_data = json.loads(line)
            valid_json_objects.append(json_data)
        except json.JSONDecodeError:
            pass
    write_json_objects(valid_json_objects, file.name)
    logging.info("File overriden with valid JSON results.")


def write_json_objects(json_objects, output_file):
    with open(output_file, "w") as f:
        first_object = True
        for obj in json_objects:
            if not first_object:
                f.write("\n")
            else:
                first_object = False
            json.dump(obj, f, separators=(",", ":"))


if __name__ == "__main__":
    logging.basicConfig(level=20)
    logging.info("Stating safe check for dbt models... ")

    file = "safe_models.json"
    logging.info("File found... ")

    clean_up_json(file)
    check_safe_models(file)

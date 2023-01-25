import yaml
from fire import Fire


class YamlReader:

    def read_seed_schema(self, dbt_project_path):

        with open(dbt_project_path, "r") as stream:
            try:
                seed_schema = yaml.safe_load(stream).get('seeds').get('+schema')
                return seed_schema
            except yaml.YAMLError as exc:
                print(exc)


    def read_seed_name(self, seed_file_path):
        with open(seed_file_path, "r") as stream:
            try:
                seed_name = yaml.safe_load(stream).get('seeds')[0]['name']
                return seed_name
            except yaml.YAMLError as exc:
                print(exc)


if __name__ == "__main__":
    yaml_reader = YamlReader()
    Fire(yaml_reader)

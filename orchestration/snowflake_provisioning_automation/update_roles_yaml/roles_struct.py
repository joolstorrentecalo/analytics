"""
Roles.yml file is converted into a standard data structure of lists and dictionaries.

This data structure needs to be manipulated, specifically in the following ways:
    - add values if the value doesn't already exist
    - remove values if the value currently exists

Note: For EACH type of user addition (roles, users, databaes),
requires new object.
And for all user removal types, requires one object only.
"""

import logging
from random import randint
from typing import Union
from utils_update_roles import DATABASES_KEY, ROLES_KEY, USERS_KEY


class RolesStruct:
    """
    Class that provides various functions to add and remove data from the data structure
    """

    def __init__(
        self,
        roles_data: dict,
        yaml_key: Union[str, None] = None,
        new_values: Union[list, None] = None,
        usernames_to_remove: Union[list, None] = None,
    ):
        self.roles_data = roles_data
        self.yaml_key = yaml_key
        self.new_values = new_values
        self.usernames_to_remove = usernames_to_remove

        # used to check if key already exists prior to adding new values
        if self.new_values:
            self.existing_value_keys = self.get_existing_value_keys()

    @staticmethod
    def get_value_keys(values: list) -> list:
        """
        `value` is a dict.
        For each value dict, get the key and save to value_keys list
        """
        value_keys = []
        for value_d in values:
            key = list(value_d.keys())[0]
            value_keys.append(key)
        return value_keys

    def get_existing_value_keys(self, yaml_key: Union[str, None] = None) -> list:
        """
        Returns all the existing keys of each dict in the list of dicts
        Tracks if a key already exists, so not to re-insert the key again
        """
        if yaml_key:
            existing_values = self.roles_data[yaml_key]
        else:
            existing_values = self.roles_data[self.yaml_key]
        return RolesStruct.get_value_keys(existing_values)

    def _add_value(self, new_value: dict):
        """Adds a new entry to the yaml file"""
        new_value_key = list(new_value.keys())[0]
        if new_value_key in self.existing_value_keys:
            logging.info(f"{new_value_key} already exists in roles.yml, skipping")
        else:
            logging.info(f"Adding {self.yaml_key} {new_value_key}")
            list_len = len(self.roles_data[self.yaml_key])
            # insert at a random position (to avoid MR conflict), but close to the end of the list
            random_insert_pos = randint(int(list_len * 0.85), list_len)
            self.roles_data[self.yaml_key].insert(random_insert_pos, new_value)
            self.existing_value_keys.append(new_value_key)

    def add_values(self):
        """Adds all new entries to the yaml file"""
        for new_value in self.new_values:
            self._add_value(new_value)

    def _filter_keys_to_remove(self, yaml_key: str, keys_to_remove: list):
        """
        Check and print out keys_to_remove that don't exist in roles.yml
        Return only filtered keys that exist in roles.yml
        """
        existing_keys = self.get_existing_value_keys(yaml_key)
        keys_to_remove_filtered = []

        for key_to_remove in keys_to_remove:
            if key_to_remove not in existing_keys:
                logging.info(
                    f"Skipping (key does not exist): Removing {yaml_key} {key_to_remove}"
                )
            else:
                keys_to_remove_filtered.append(key_to_remove)
        return keys_to_remove_filtered

    def _pop_values(self, yaml_key: str, keys_to_remove: list):
        """
        From the yaml file, removes any entry that matches
        list `keys_to_remove`
        """
        existing_values = self.roles_data[yaml_key]

        # check and print out keys_to_remove that don't exist in roles.yml
        keys_to_remove_filtered = self._filter_keys_to_remove(yaml_key, keys_to_remove)

        # iterate backwards, popping any matching values slated for removal
        for i in range(len(existing_values) - 1, -1, -1):
            existing_value = existing_values[i]
            existing_value_key = list(existing_value.keys())[0]
            if existing_value_key in keys_to_remove_filtered:
                logging.info(f"Removing {yaml_key} {existing_value_key}")
                self.roles_data[yaml_key].pop(i)

    def _remove_databases(self):
        """Creates a list of all databases to check for removal"""
        keys_to_remove = []
        for username in self.usernames_to_remove:
            prep = f"{username}_prep"
            prod = f"{username}_prod"
            keys_to_remove = keys_to_remove + [prep, prod]
        self._pop_values(DATABASES_KEY, keys_to_remove)

    def _remove_roles(self):
        """Creates a list of all roles to check for removal"""
        keys_to_remove = []
        for username in self.usernames_to_remove:
            keys_to_remove.append(username)
        self._pop_values(ROLES_KEY, keys_to_remove)

    def _remove_users(self):
        """Creates a list of all users to check for removal"""
        keys_to_remove = []
        for username in self.usernames_to_remove:
            keys_to_remove.append(username)
        self._pop_values(USERS_KEY, keys_to_remove)

    def remove_values(self):
        """Removes databaes/roles/users for any passed in usernames to remove"""
        if self.usernames_to_remove:
            self._remove_databases()
            self._remove_roles()
            self._remove_users()
        else:
            logging.info(
                f"\nNo values to remove- 'usernames_to_remove' is empty: {self.usernames_to_remove}"
            )

import os
import yaml
import pytest

import roles_struct


def get_roles_data():
    """function to read in test roles.yml file as py obj"""
    script_dir = os.path.dirname(os.path.realpath(__file__))
    roles_file_name = "roles_test.yml"
    file_path = os.path.join(script_dir, roles_file_name)

    with open(file_path, "r", encoding="utf-8") as yaml_file:
        roles_data = yaml.safe_load(yaml_file)
    return roles_data


@pytest.fixture
def my_roles_struct():
    """
    Creates a fixture instance of RolesStruct to be used by all test functions
    This instance is used specifically to test database additions
    """
    roles_data = get_roles_data()
    yaml_key = "databases"
    new_values = [{"user3_prod": {"shared": False}}, {"user3_prep": {"shared": False}}]
    usernames_to_remove = ["user1", "user2"]

    obj = roles_struct.RolesStruct(
        roles_data, yaml_key, new_values, usernames_to_remove
    )
    return obj


def test_add_values(my_roles_struct):
    """
    Test adding new database values:
    1. happy path: value is added to the end of the list for the correct key
    2. if the key already exists, make sure that it's not added
    """
    # Test 1: test that the new values are inserted at the end
    my_roles_struct.add_values()
    databases = my_roles_struct.roles_data["databases"]
    original_len = len(databases)
    assert {"user3_prep": {"shared": False}} in databases
    assert {"user3_prod": {"shared": False}} in databases

    # Test 2: adding values (duplicates) doesn't insert any new values
    my_roles_struct.add_values()
    databases = my_roles_struct.roles_data["databases"]
    new_len = len(databases)
    assert original_len == new_len


def test_pop_values(my_roles_struct):
    """
    Test `_pop_values`:

    1. happy path: value is removed from the list
    2. edge cases: make sure that matches at both the first and last position are also correctly removed
    3. edgecase2: make sure that if the key exists more than once, that both keys are removed correctly
    """

    # Test 1: make sure keys are removed successfully
    yaml_key = "databases"
    keys_to_remove = ["user1_prep", "user1_prod"]
    original_value_keys = my_roles_struct.get_existing_value_keys(yaml_key)
    # check that key originally exists
    for key_to_remove in keys_to_remove:
        assert key_to_remove in original_value_keys

    databases = my_roles_struct.roles_data[yaml_key]
    original_len = len(databases)
    my_roles_struct._pop_values(yaml_key, keys_to_remove)
    remaining_value_keys = my_roles_struct.get_existing_value_keys(yaml_key)
    # check the key has now been removed
    for key_to_remove in keys_to_remove:
        assert key_to_remove not in remaining_value_keys

    # Test 2: test that duplicate keys are removed as well
    # There are 2 keys to remove, but 4 instances of those keys exist
    databases = my_roles_struct.roles_data[yaml_key]
    new_len = len(databases)
    assert original_len == new_len + 4

    # Test 3: test removal of all items in 'roles'
    yaml_key = "roles"
    keys_to_remove = my_roles_struct.get_existing_value_keys(yaml_key)
    my_roles_struct._pop_values(yaml_key, keys_to_remove)
    roles = my_roles_struct.roles_data[yaml_key]
    assert len(roles) == 0

    # Test 4: remove the first item in users (user1)
    yaml_key = "users"
    keys_to_remove = ["user1"]
    original_value_keys = my_roles_struct.get_existing_value_keys(yaml_key)
    # check that key originally exists
    for key_to_remove in keys_to_remove:
        assert key_to_remove in original_value_keys

    my_roles_struct._pop_values(yaml_key, keys_to_remove)
    remaining_value_keys = my_roles_struct.get_existing_value_keys(yaml_key)
    # check that key has been now removed
    for key_to_remove in keys_to_remove:
        assert key_to_remove not in remaining_value_keys

    # Test 5: remove the last item in users (user3)
    yaml_key = "users"
    keys_to_remove = ["user3"]
    original_value_keys = my_roles_struct.get_existing_value_keys(yaml_key)
    # check that key originally exists
    for key_to_remove in keys_to_remove:
        assert key_to_remove in original_value_keys

    my_roles_struct._pop_values(yaml_key, keys_to_remove)
    remaining_value_keys = my_roles_struct.get_existing_value_keys(yaml_key)
    # check that key has been now removed
    for key_to_remove in keys_to_remove:
        assert key_to_remove not in remaining_value_keys


def test_filter_keys_to_remove(my_roles_struct):
    """
    Test that for keys that don't exist
    Skip trying to pop them from the roles data struct
    """
    yaml_key = "roles"
    keys_to_remove = ["user1", "user_not_exist"]
    keys_to_remove_filtered = my_roles_struct._filter_keys_to_remove(
        yaml_key, keys_to_remove
    )
    assert keys_to_remove_filtered == ["user1"]


def test_get_existing_value_keys(my_roles_struct):
    """
    Test that get_existing_value_keys() returns the correct keys
    even after add/remove
    """
    yaml_key = "databases"
    # Test 1: get_existing_value_keys() returns the current state
    existing_value_keys1 = my_roles_struct.get_existing_value_keys(yaml_key)
    assert existing_value_keys1 == [
        "user1_prod",
        "user1_prep",
        "covid19",
        "user1_prod",
        "user1_prep",
        "user2_prep",
        "user2_prod",
    ]

    # Test 2: get_existing_value_keys() returns the state after adding keys
    my_roles_struct.add_values()
    existing_value_keys2 = my_roles_struct.get_existing_value_keys(yaml_key)
    keys_to_check = existing_value_keys1 + ["user3_prod", "user3_prep"]
    assert len(keys_to_check) == len(existing_value_keys2)
    for key_to_check in keys_to_check:
        assert key_to_check in existing_value_keys2

    # Test 3: get_existing_value_keys() returns the state after removing keys
    my_roles_struct._remove_databases()
    existing_value_keys3 = my_roles_struct.get_existing_value_keys(yaml_key)
    assert len(existing_value_keys3) == 3
    keys_to_check = ["covid19", "user3_prod", "user3_prep"]
    for key_to_check in keys_to_check:
        assert key_to_check in existing_value_keys3

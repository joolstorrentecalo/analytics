import pytest
from render_templates import render_template_for_username, concat_template_values


def test_render_template_for_username():
    # Test 1: check that jinja rendering works
    username = "some_username"
    template_content = '{ "username": "{{ username }}" }'
    value = render_template_for_username(username, template_content)
    assert value == {"username": username}

    # Test 2: should still work if no jinja templating is needed
    template_content = '{ "username": "another_username" }'
    value = render_template_for_username(username, template_content)
    assert value == {"username": "another_username"}

    # Test 3: if context key is invalid, should throw an error
    username = "some_username"
    invalid_template_content = '{ "username": "{{ username }}" '
    with pytest.raises(ValueError):
        render_template_for_username(username, invalid_template_content)


def test_concat_template_values():
    usernames = ["user1", "user2"]
    dict_template = '{"prod_database_name": "{{ prod_database }}"}'
    values = concat_template_values(usernames, dict_template)
    expected_value1 = {"prod_database_name": "user1_prod"}
    expected_value2 = {"prod_database_name": "user2_prod"}
    assert values == [expected_value1, expected_value2]

    # Test 2: test values using list template
    usernames = ["user1", "user2"]
    list_template = '["{{ prod_database }}"]'
    values = concat_template_values(usernames, list_template)
    expected_value1 = "user1_prod"
    expected_value2 = "user2_prod"
    assert values == [expected_value1, expected_value2]

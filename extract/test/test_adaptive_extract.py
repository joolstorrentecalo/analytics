"""
Test Adaptive extract
"""
from unittest.mock import Mock

import pandas as pd
import pytest
from extract.adaptive.src.adaptive import Adaptive
from extract.adaptive.src.adaptive_helpers import edit_dataframe


@pytest.fixture(scope="session", name="adaptive_object")
def adaptive_obj():
    """Create Adaptive object to be used in further tests"""
    adaptive = Adaptive()
    return adaptive


def test_handle_response(adaptive_object):
    """Test _base_xml()"""
    response = Mock()

    # Test if successful response, return csv data
    response.text = """
    <response success="not false">
      <output>some_csv_data</output>
    </response>
    """
    export_output = adaptive_object.handle_response(response)
    assert export_output == "some_csv_data"

    # Test if successful response, return csv data
    response.text = """
    <response success="false">
      <output>some_csv_data</output>
    </response>
    """

    response.text = """
    <response success="false">
      <messages>
        <message key="error-authentication-failure">Authentication failed for User: some_user@gitlab.com</message>
      </messages>
    </response>
    """
    with pytest.raises(SystemExit) as pytest_wrapped_e:
        export_output = adaptive_object.handle_response(response)
    assert pytest_wrapped_e.type == SystemExit


def test_iterate_versions(adaptive_object):
    """
    Test that the version names from the (nested) json object are added/parsed correctly
    """
    version_reports = []
    versions = [
        {"@id": "783", "@name": "FY22 - 1+11 Forecast", "@type": "PLANNING"},
        {"@id": "883", "@name": "FY22 - 2+10 Forecast", "@type": "PLANNING"},
        {
            "@id": "806",
            "@name": "FY22 PLAN Versions",
            "@type": "VERSION_FOLDER",
            "version": [
                {"@id": "703", "@name": "FY22 - BOD PLAN", "@type": "PLANNING"},
                {
                    "@id": "1083",
                    "@name": "FY22 - Adjusted BOD PLAN",
                    "@type": "PLANNING",
                },
            ],
        },
    ]
    adaptive_object._iterate_versions(versions, version_reports)
    print(version_reports)
    assert sorted(version_reports) == [
        "FY22 - 1+11 Forecast",
        "FY22 - 2+10 Forecast",
        "FY22 - Adjusted BOD PLAN",
        "FY22 - BOD PLAN",
    ]


def test_filter_for_subfolder(adaptive_object):
    """
    Test filter for subfolder
    """
    versions = [
        {"@id": "883", "@name": "FY22 - 2+10 Forecast", "@type": "PLANNING"},
        {
            "@id": "806",
            "@name": "FY22 PLAN Versions",
            "@type": "VERSION_FOLDER",
            "version": [
                {"@id": "703", "@name": "FY22 - BOD PLAN", "@type": "PLANNING"},
                {
                    "@id": "1083",
                    "@name": "FY22 - Adjusted BOD PLAN",
                    "@type": "PLANNING",
                },
            ],
        },
        {
            "@id": "1",
            "@name": "FY23 PLAN Versions",
            "@type": "VERSION_FOLDER",
            "version": [
                {"@id": "2", "@name": "FY23 - BOD PLAN", "@type": "PLANNING"},
                {"@id": "3", "@name": "FY23 - Adjusted BOD PLAN", "@type": "PLANNING"},
            ],
        },
    ]
    folder_criteria = "FY23 PLAN Versions"
    res = adaptive_object._filter_for_subfolder(versions, folder_criteria)
    print(f"\nres: {res}")
    assert res == [
        {"@id": "2", "@name": "FY23 - BOD PLAN", "@type": "PLANNING"},
        {"@id": "3", "@name": "FY23 - Adjusted BOD PLAN", "@type": "PLANNING"},
    ]


def test_exported_data_to_df(adaptive_object):
    """
    The exportData response returns a string including some csv data
    Test that the csv data can be converted to pandas dataframe
    """

    exported_data_test = """
    Account Name,Account Code,Level Name,02/2018,03/2018,04/2018,05/2018,06/2018,07/2018,08/2018,09/2018,10/2018,11/2018,12/2018,01/2019,02/2019,03/2019,04/2019,05/2019,06/2019,07/2019,08/2019,09/2019,10/2019,11/2019,12/2019,01/2020,02/2020,03/2020,04/2020,05/2020,06/2020,07/2020,08/2020,09/2020,10/2020,11/2020,12/2020,01/2021,02/2021,03/2021,04/2021,05/2021,06/2021,07/2021,08/2021,09/2021,10/2021,11/2021,12/2021,01/2022,02/2022,03/2022,04/2022,05/2022,06/2022,07/2022,08/2022,09/2022,10/2022,11/2022,12/2022,01/2023,02/2023,03/2023,04/2023,05/2023,06/2023,07/2023,08/2023,09/2023,10/2023,11/2023,12/2023,01/2024,02/2024,03/2024,04/2024,05/2024,06/2024,07/2024,08/2024,09/2024,10/2024,11/2024,12/2024,01/2025,02/2025,03/2025,04/2025,05/2025,06/2025,07/2025,08/2025,09/2025,10/2025,11/2025,12/2025,1/2026,2/2026,3/2026,4/2026,5/2026,6/2026,7/2026,8/2026,9/2026,10/2026,11/2026,12/2026,1/2027
    "Total Headcount Expense","HeadcountExpenseFcst","1",0.0,0.0,1155.67,0.0,0.0,454.03,0.0,0.0,578.06,0.0,-1.5508313441000325,3985.9383838265994,5990.27,6772.25,7500.49,8871.54,8683.21,9338.62,10536.28,11960.38,15958.38,15067.58,250084.3358183593,14474.1162154893,19624.702011278303,22462.726901573995,21858.508889540797,22226.2256212693,21314.352907802004,24093.6826698202,18712.531866558496,27799.2360394795,27837.236619098796,27914.143601739295,892871.2549109808,27794.838822527694,35159.248242351205,42388.39336342229,74837.531391191,67143.36,63248.46,107903.27,79699.96,78029.18,173520.08,78029.18,104099.51,396102.95,189017.43,197491.46,403074.79,401217.1,392163.35,765949.87,442568.96,335469.36,441338.87,418416.08,437092.52,571399.54,371233.89,585033.08,432106.8394921232,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0
    """
    dataframe = adaptive_object.exported_data_to_df(exported_data_test)
    assert "Account Code" in dataframe.columns


def test_edit_dataframe():
    """Check that the edited dataframe has the correct num of cols"""
    version = "some_version1"
    data = {
        "Account Name": ["some_account"],
        "Account Code": ["some_account_code"],
        "Level Name": ["some_level_name"],
        "06/2023": ["some_value"],
        "07/2023": ["some_value"],
    }
    dataframe = pd.DataFrame(data)
    edited_dataframe = edit_dataframe(dataframe, version)
    expected_final_cols = [
        "ACCOUNT_NAME",
        "ACCOUNT_CODE",
        "CALENDAR_MONTH",
        "CALENDAR_YEAR",
        "LEVEL_NAME",
        "VALUE",
        "VERSION",
    ]

    assert set(edited_dataframe.columns) == set(expected_final_cols)

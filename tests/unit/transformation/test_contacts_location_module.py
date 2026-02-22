import pandas as pd
from include.etl.transformation.core_transformation.modules.contacts_location import (
    transform_contacts_location_module,
)


def test_return_count(study_key, full_study_data):
    result = transform_contacts_location_module(study_key, full_study_data)
    print(f"Return count: {len(result)}")
    print(f"Types: {[type(r) for r in result]}")
    assert len(result) == 6


def test_empty_data_returns_empty_lists(study_key, empty_study_data):
    """Test empty study data returns empty lists."""
    (
        contacts,
        study_contacts,
        locations,
        study_locations,
        location_contacts,
        study_location_contacts,
    ) = transform_contacts_location_module(study_key, empty_study_data)

    assert contacts == []
    assert study_contacts == []
    assert locations == []
    assert study_locations == []
    assert location_contacts == []
    assert study_location_contacts == []


def test_central_contacts_extracted(study_key, full_study_data):
    """Test central contacts are properly extracted."""
    contacts, study_contacts, *_ = transform_contacts_location_module(
        study_key, full_study_data
    )

    assert len(contacts) == 1
    assert contacts[0]["name"] == "Dr. John Smith"
    assert contacts[0]["role"] == "CONTACT"
    assert contacts[0]["email"] == "jsmith@example.com"


def test_locations_extracted_with_geopoint(study_key, full_study_data):
    """Test locations are extracted including geocoordinates."""
    _, _, locations, study_locations, _, _ = transform_contacts_location_module(
        study_key, full_study_data
    )

    assert len(locations) == 1
    assert locations[0]["facility"] == "City Hospital"
    assert locations[0]["city"] == "Boston"
    assert locations[0]["lat"] == 42.3601
    assert locations[0]["lon"] == -71.0589


def test_locations_extracted_with_contacts(study_key, full_study_data):
    """Test locations are extracted including contacts."""
    _, _, _, _, location_contacts, study_location_contacts = (
        transform_contacts_location_module(study_key, full_study_data)
    )

    assert len(location_contacts) == 2
    assert location_contacts[0]["name"] == "Dr. Olusola"
    assert location_contacts[0]["role"] == "Coordinator"
    assert location_contacts[0]["phone"] == "90909090"
    assert location_contacts[0]["phone_ext"] == "+234"
    assert location_contacts[0]["email"] == "olusola@hospital.com"


def test_study_locations_has_status(study_key, full_study_data):
    """Test study-location bridge has recruitment status."""
    _, _, _, study_locations, _, _ = transform_contacts_location_module(
        study_key, full_study_data
    )

    assert study_locations[0]["status"] == "RECRUITING"


def test_geopoint_with_none_values(study_key):
    """Test GeoPoint with None lat/lon is handled."""

    study_data = pd.Series(
        {
            "protocolSection.contactsLocationsModule.locations": [
                {
                    "facility": "Hospital",
                    "city": "City",
                    "state": "State",
                    "country": "Country",
                    "geoPoint": {"lat": None, "lon": None},
                },
            ],
        }
    )

    _, _, locations, _, _, _ = transform_contacts_location_module(study_key, study_data)

    assert locations[0]["lat"] is None
    assert locations[0]["lon"] is None


def test_none_in_nested_dict(study_key):
    """Test None values in nested dicts are handled gracefully."""

    study_data = pd.Series(
        {
            "protocolSection.contactsLocationsModule.locations": [
                {
                    "facility": "Hospital",
                    "city": "City",
                    "state": None,
                    "country": "Country",
                    "geoPoint": None,
                },
            ],
        }
    )

    _, _, locations, _, _, _ = transform_contacts_location_module(study_key, study_data)

    assert len(locations) == 1
    assert locations[0]["state"] is None
    assert "lat" not in locations[0]

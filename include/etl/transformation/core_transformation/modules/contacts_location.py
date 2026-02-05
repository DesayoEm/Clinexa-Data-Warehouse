from typing import Tuple
import logging
import pandas as pd
import numpy as np
from include.etl.transformation.config import NON_SCALAR_FIELDS
from include.etl.transformation.utils import generate_key

log = logging.getLogger("airflow.task")


def transform_contacts_location_module(study_key: str, study_data: pd.Series) -> Tuple:
    """
    Extract central contacts and study site locations from a clinical trial.

    Central contacts are the primary points of contact for trial inquiries
    Locations are the physical sites where thE trial is conducted,
    which may include hospitals, clinics, or research centers across multiple countries.

    Location records include optional geocoordinates when available, enabling
    geographic analysis and proximity-based patient matching.

    Site-specific contact information and recruitment status are stored in the bridge table
    as they represent the study-location relationship rather than the location
    itself.

    Args:
        study_key: Unique identifier for the clinical trial study.
        study_data: Flattened study record containing nested contact and
            location data

    Returns:
        Four-element tuple containing:
            - central_contacts: Contact dimension records
            - study_central_contacts: Bridge table linking studies to contacts
            - locations: Location dimension records
            - study_locations: Bridge table with study-specific location data


        All lists return empty if no contacts/locations exist for the study.
    """

    central_contacts = []
    study_central_contacts = []
    locations = []
    study_locations = []

    contacts_locations_index = NON_SCALAR_FIELDS["contacts_location"]["index_field"]

    # contacts
    central_contacts_list = study_data.get(
        f"{contacts_locations_index}.centralContacts"
    )

    if (
        isinstance(central_contacts_list, (list, np.ndarray))
        and len(central_contacts_list) > 0
    ):

        for central_contact in central_contacts_list:
            name = central_contact.get("name")
            role = central_contact.get("role")
            phone = central_contact.get("phone")
            phone_ext = central_contact.get("phoneExt")
            email = central_contact.get("email")

            central_contact_key = generate_key(name, role, phone, phone_ext, email)

            central_contacts.append(
                {
                    "contact_key": central_contact_key,
                    "name": name,
                    "role": role,
                    "phone": phone,
                    "phone_ext": phone_ext,
                    "email": email,
                }
            )

            study_central_contacts.append(
                {
                    "study_key": study_key,
                    "contact_key": central_contact_key,
                }
            )

    # locations
    locations_list = study_data.get(f"{contacts_locations_index}.locations")

    if isinstance(locations_list, (list, np.ndarray)) and len(locations_list) > 0:
        for location in locations_list:
            facility = location.get("facility")
            city = location.get("city")
            state = location.get("state")
            country = location.get("country")

            location_key = generate_key(facility, city, state, country)
            curr_location = {
                "location_key": location_key,
                "facility": facility,
                "city": city,
                "state": state,
                "country": country,
            }
            geopoint = location.get("geoPoint")

            if isinstance(geopoint, dict) and geopoint:
                curr_location["lat"] = (
                    float(geopoint.get("lat")) if geopoint.get("lat") else None
                )
                curr_location["lon"] = (
                    float(geopoint.get("lon")) if geopoint.get("lon") else None
                )

            locations.append(curr_location)

            study_locations.append(
                {
                    "study_key": study_key,
                    "location_key": location_key,
                    "status": location.get("status"),
                    "contacts": location.get("contacts"),  # json blob
                }
            )

    return central_contacts, study_central_contacts, locations, study_locations

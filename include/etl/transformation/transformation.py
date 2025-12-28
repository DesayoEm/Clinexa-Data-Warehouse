from typing import Dict, List, Tuple, Hashable
import logging
import pandas as pd
import numpy as np
import hashlib
import json

from transformer_config import SINGLE_FIELDS, NESTED_FIELDS
from data_cleaning import Cleaner
from airflow.utils.context import Context
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


class Transformer:
    def __init__(self, context: Context, s3_dest_hook: S3Hook = None):
        self.context = context
        self.execution_date = self.context.get("ds")
        self.log = logging.getLogger("airflow.task")

        self.s3 = s3_dest_hook or S3Hook(aws_conn_id="aws_airflow")
        self.cleaner = Cleaner()

    @staticmethod
    def generate_key(*args) -> str:
        """Generates a deterministic surrogate key from input values."""
        combined = "|".join(str(arg) for arg in args if arg is not None)
        return hashlib.sha256(combined.encode()).hexdigest()[:16]

    def transform_all_studies(self, folder: str) -> None:
        for study_file in folder:
            study_file_loc = ""
            try:
                # pull from s3
                self.transform_study_file(study_file_loc)
                # checkpoint

            # inner loop will fail gracefully wherever possible and errors will only raise for critical issues
            except Exception as e:
                raise

    def transform_study_file(self, data_loc: str):
        """
        Transform a batch of raw study dicts in a file.
        Args:
            data_loc: Location of the  file
        """
        all_studies = []
        all_sponsors = []
        all_study_sponsors = []

        all_conditions = []
        all_study_conditions = []

        all_keywords = []
        all_study_keywords = []

        all_study_arm_group_interventions = []
        all_interventions = []
        all_study_interventions = []

        all_locations = []
        all_study_locations = []
        all_central_contacts = []
        all_study_central_contacts = []



        df_studies = pd.read_parquet(data_loc)
        df_studies = pd.json_normalize(df_studies["studies"])

        for idx, study in df_studies.iterrows():
            nct_index = SINGLE_FIELDS['nct_id']
            nct_id = study.get(nct_index)
            
            if not nct_id:
                self.log.warning(f"Study missing NCT ID, skipping {idx}")  
                continue

            study_key = self.generate_key(nct_id)

            # study
            study_record = self.extract_study_fields(study_key, study)
            all_studies.append(study_record)

            # sponsors
            sponsors, study_sponsors = self.extract_sponsors(idx, study_key, study)
            all_sponsors.extend(sponsors)
            all_study_sponsors.extend(study_sponsors)

            # conditions and keywords
            conditions, study_conditions = self.extract_conditions(idx, study_key, study)
            all_conditions.extend(conditions)
            all_study_conditions.extend(study_conditions)

            keywords, study_keywords = self.extract_keywords(idx, study_key, study)
            all_keywords.extend(keywords)
            all_study_keywords.extend(study_keywords)

            # groups and interventions
            arm_group_interventions = self.extract_arm_groups(idx, study_key, study)
            all_study_arm_group_interventions.extend(arm_group_interventions)

            interventions, study_interventions = self.extract_interventions(idx, study_key, study)
            all_interventions.extend(interventions)
            all_study_interventions.extend(study_interventions)

            # contacts and locations
            central_contacts, study_central_contacts  = self.extract_central_contacts(idx, study_key, study)
            all_central_contacts.extend(central_contacts)
            all_study_central_contacts.extend(study_central_contacts)

            locations, study_locations = self.extract_locations(idx, study_key, study)
            all_locations.extend(locations)
            all_study_locations.extend(study_locations)





        df_studies = pd.DataFrame(all_studies)

        #sponsors and collaborators
        df_sponsors = pd.DataFrame(all_sponsors)
        df_study_sponsors = pd.DataFrame(all_study_sponsors)

        #conditions and keywords
        df_conditions = pd.DataFrame(all_conditions)
        df_study_conditions = pd.DataFrame(all_study_conditions)
        df_keywords = pd.DataFrame(all_keywords)
        df_study_keywords = pd.DataFrame(all_study_keywords)

        #arms and interventions
        df_interventions = pd.DataFrame(all_interventions)
        df_study_interventions = pd.DataFrame(all_study_interventions)
        df_arm_group_interventions = pd.DataFrame(
            all_study_arm_group_interventions
        )

        # contacts and locations
        df_central_contacts = pd.DataFrame(all_central_contacts)
        df_study_central_contacts = pd.DataFrame(all_study_central_contacts)
        df_locations = pd.DataFrame(all_locations)
        df_study_locations = pd.DataFrame(all_study_locations)

        # dedupe
        df_sponsors = df_sponsors.drop_duplicates(subset=["sponsor_key"])
        df_conditions = df_conditions.drop_duplicates(subset=["condition_key"])
        df_keywords = df_keywords.drop_duplicates(subset=["keyword_key"])
        df_interventions = df_interventions.drop_duplicates(subset=["intervention_key"])
        df_arm_group_interventions = df_arm_group_interventions.drop_duplicates(subset=["study_arm_key", "study_key" , "arm_intervention_name"])
        df_locations = df_locations.drop_duplicates(subset=["location_key"])
        df_central_contacts = df_central_contacts.drop_duplicates(subset=["contact_key"])

        # load
        return df_studies, df_sponsors, df_study_sponsors

    @staticmethod
    def extract_study_fields(study_key: str, study_data: pd.Series) -> Dict:
        study_record = dict()

        study_record["study_key"] = study_key
        for entity_key in SINGLE_FIELDS:
            index_field = SINGLE_FIELDS.get(entity_key)

            study_record[entity_key] = study_data.get(index_field)

        return study_record


    def extract_sponsors(self, idx: Hashable, study_key: str, study_data: pd.Series):
        """
        Extract sponsors from a single study.
        Args:
            idx: The index of the study on the current page
            study_key: The generated key for this study
            study_data: The nested dict for one study (not a DataFrame)
        Returns:
            Tuple of (sponsors_list, study_sponsors_list)
        """

        sponsors = []
        study_sponsors = []

        # Extract lead sponsor
        lead_sponsor_index = NESTED_FIELDS["sponsor"]["index_field"]

        # sponsor name and class are scalar values and MUST be extracted directly
        lead_sponsor_name = study_data.get(f'{lead_sponsor_index}.name')
        lead_sponsor_class = study_data.get(f'{lead_sponsor_index}.class')

        if pd.notna(lead_sponsor_name) and pd.notna(lead_sponsor_class):
            sponsor_key = self.generate_key(
                lead_sponsor_name, lead_sponsor_class
            )
            sponsors.append(
                {
                    "sponsor_key": sponsor_key,
                    "name": lead_sponsor_name,
                    "sponsor_class": lead_sponsor_class,
                }
            )

            study_sponsors.append(
                {"study_key": study_key, "sponsor_key": sponsor_key, "is_lead": True}
            )
        else:
            self.log.warning(f"No lead sponsor found for study {study_key}, page - index {idx}")


        # Extract collaborators
        collaborators_index = NESTED_FIELDS["collaborators"]["index_field"]
        collaborators_list = study_data.get(collaborators_index)

        if isinstance(collaborators_list, (list, np.ndarray)) and len(collaborators_list) > 0:
            for collaborator in collaborators_list:
                sponsor_key = self.generate_key(
                    collaborator.get("name"), collaborator.get("class")
                )

                sponsors.append(
                    {
                        "sponsor_key": sponsor_key,
                        "name": collaborator.get("name"),
                        "sponsor_class": collaborator.get("class"),
                    }
                )

                study_sponsors.append(
                    {"study_key": study_key, "sponsor_key": sponsor_key, "is_lead": False}
                )

        return sponsors, study_sponsors


    def extract_conditions(self, idx: Hashable, study_key: str, study_data: pd.Series) -> Tuple | None:
        conditions = []
        study_conditions = []

        conditions_index = NESTED_FIELDS["conditions"]["index_field"]
        conditions_list = study_data.get(conditions_index)


        if isinstance(conditions_list, (list, np.ndarray)) and len(conditions_list) > 0:
            for condition in conditions_list:
                condition_key = self.generate_key(condition)

                conditions.append(
                    {"condition_key": condition_key, "condition_name": condition}
                )

                study_conditions.append(
                    {
                        "study_key": study_key,
                        "condition_key": condition_key,
                    }
                )

        else:
            self.log.warning(f"No conditions found for study {study_key}, page - index {idx}")

        return conditions, study_conditions


    def extract_keywords(self, idx: Hashable, study_key: str, study_data: pd.Series) -> Tuple:
        keywords = []
        study_keywords = []

        keywords_index = NESTED_FIELDS["keywords"]["index_field"]
        keywords_list = study_data.get(keywords_index)

        if isinstance(keywords_list, (list, np.ndarray)) and len(keywords_list) > 0:

            for keyword in keywords_list:
                keyword_key = self.generate_key(keyword)

                keywords.append({"keyword_key": keyword_key, "keyword_name": keyword})

                study_keywords.append(
                    {
                        "study_key": study_key,
                        "keyword_key": keyword_key,
                    }
                )
        return keywords, study_keywords


    def extract_interventions(self, idx: Hashable, study_key: str, study_data: pd.Series) -> Tuple | None:
        intervention_names = []
        study_interventions = []

        interventions_index = NESTED_FIELDS["interventions"]["index_field"]
        interventions_list = study_data.get(interventions_index)

        if isinstance(interventions_list, (list, np.ndarray)) and len(interventions_list) > 0:
            for intervention in interventions_list:
                main_name = intervention.get("name")
                intervention_type = intervention.get("type")
                description = intervention.get("description")

                intervention_key = self.generate_key(main_name, intervention_type)
                intervention_names.append({
                    "intervention_key": intervention_key,
                    "intervention_name": main_name,
                    "intervention_type": intervention_type,
                    "description": description,
                    "is_primary_name": True
                })

                study_interventions.append({
                    "study_key": study_key,
                    "intervention_key": intervention_key,
                    "is_primary_name": True
                })


                other_names = intervention.get("otherNames")
                if isinstance(other_names, (list, np.ndarray)) and len(other_names) > 0:
                    for other_name in other_names:
                        if other_name == main_name:
                            continue  # some studies put the main name in the list of other names

                        intervention_key = self.generate_key(other_name, intervention_type)
                        intervention_names.append({
                            "intervention_key": intervention_key,
                            "intervention_name": other_name,
                            "intervention_type": intervention_type,
                            "description": description,  # inherits from parent

                        })

                        study_interventions.append({
                            "study_key": study_key,
                            "intervention_key": intervention_key,
                            "is_primary_name": False
                        })
        else:
            pass
            # self.log.warning(f"No interventions found for study {study_key}, {idx}") #noisy


        return intervention_names, study_interventions


    def extract_arm_groups(self, idx: Hashable, study_key: str, study_data: pd.Series) -> List | None:
        study_arms_interventions = []

        study_arms_index = NESTED_FIELDS["arm_groups"]["index_field"]
        study_arms_list = study_data.get(study_arms_index)

        if isinstance(study_arms_list, (list, np.ndarray)) and len(study_arms_list) > 0:
            for study_arm in study_arms_list:
                study_arm_label = study_arm.get("label")
                study_arm_description = study_arm.get("description")
                study_arm_type = study_arm.get("type")

                arm_intervention_key = self.generate_key(study_key, study_arm_label, study_arm_description,
                                                         study_arm_type)

                arm_interventions = study_arm.get("interventionNames")
                if isinstance(arm_interventions, (list, np.ndarray)) and len(arm_interventions) > 0:

                    for intervention in arm_interventions:
                        study_arms_interventions.append(
                            {
                                "study_key": study_key,
                                "arm_intervention_key": arm_intervention_key,
                                "arm_label": study_arm_label,
                                "arm_description": study_arm_description,
                                "arm_type": study_arm_type,
                                "arm_intervention_name": intervention,
                            }
                        )
                else:
                    study_arms_interventions.append(
                        {
                            "study_key": study_key,
                            "arm_intervention_key": arm_intervention_key,
                            "arm_label": study_arm_label,
                            "arm_description": study_arm_description,
                            "arm_type": study_arm_type,
                            "arm_intervention_name": None,
                        }
                    )

        # else:
        #     self.log.warning(f"No arms found for study {study_key}, page - index {idx}") #too noisy

        return study_arms_interventions



    def extract_central_contacts(self, idx: Hashable, study_key: str, study_data: pd.Series) -> Tuple:
        central_contacts = []
        study_central_contacts = []

        central_contacts_index = NESTED_FIELDS["central_contacts"]["index_field"]
        central_contacts_list = study_data.get(central_contacts_index)

        if isinstance(central_contacts_list, (list, np.ndarray)) and len(central_contacts_list) > 0:

            for central_contact in central_contacts_list:
                name = central_contact.get("name")
                role = central_contact.get("role")
                phone = central_contact.get("phone")
                email = central_contact.get("email")

                central_contact_key = self.generate_key(name, role, phone, email)

                central_contacts.append(
                    {"contact_key": central_contact_key,
                     "contact_name": name,
                     "contact_role": role,
                     "contact_phone": phone,
                     "contact_email": email,
                     })

                study_central_contacts.append(
                    {
                        "study_key": study_key,
                        "contact_key": central_contact_key,
                    }
                )

        return central_contacts, study_central_contacts

    def extract_locations(self, idx: Hashable, study_key: str, study_data: pd.Series) -> Tuple:
        """
        Extract locations and stores location contact as JSON blob.

        NOTE: Officials are stored denormalized as JSON since not used for filtering/analysis.
        Avoids snowflaking the schema while preserving all contact information for downstream applications.
        """
        locations = []
        study_locations = []

        locations_index = NESTED_FIELDS["locations"]["index_field"]
        locations_list = study_data.get(locations_index)


        if isinstance(locations_list, (list, np.ndarray)) and len(locations_list) > 0:

            for location in locations_list:
                facility = location.get("facility")
                city = location.get("city")
                state = location.get("state")
                country = location.get("country")

                location_key = self.generate_key(facility, city, state, country)

                curr_location = {
                    "location_key": location_key,
                    "status": location.get("status"),
                    "facility": facility,
                    "city": city,
                    "state": state,
                    "country": country,
                    "contacts": location.get("contacts"),  # json blob
                }

                geopoint = location.get("geoPoint")
                if isinstance(geopoint, (dict, np.ndarray)) and len(geopoint) > 0:
                    curr_location["lat"] = geopoint.get("lat"),
                    curr_location["lon"] = geopoint.get("lon")

                locations.append(curr_location)


            study_locations.append(
                    {
                        "study_key": study_key,
                        "location_key": location_key,
                    }
                )

        return locations, study_locations




    def extract_outcomes(self, study_key: str, study_data: pd.Series) -> Tuple:
        pass

    def extract_see_also(self, idx: Hashable, study_key: str, study_data: pd.Series) -> Tuple:
        pass

    def extract_study_phases(self, study_key: str, study_data: pd.Series) -> Tuple:
        pass

    def extract_age_group(self, study_key: str, study_data: pd.Series) -> Tuple:
        pass

    def extract_ipd_info_types(self, study_key: str, study_data: pd.Series) -> Tuple:
        pass

    def extract_id_infos(self, idx: Hashable, study_key: str, study_data: pd.Series) -> Tuple:
        pass

    def extract_nct_id_aliases(self, study_key: str, study_data: pd.Series) -> Tuple:
        pass

    def extract_condition_mesh(self, study_key: str, study_data: pd.Series) -> Tuple:
        pass

    def extract_intervention_mesh(self, study_key: str, study_data: pd.Series) -> Tuple:
        pass

    def extract_large_documents(self, idx: Hashable, study_key: str, study_data: pd.Series) -> Tuple:
        pass

    def extract_unposted_events(self, idx: Hashable, study_key: str, study_data: pd.Series) -> Tuple:
        pass

    def extract_violation_events(self, idx: Hashable, study_key: str, study_data: pd.Series) -> Tuple:
        pass

    def extract_removed_countries(self, idx: Hashable, study_key: str, study_data: pd.Series) -> Tuple:
        pass

    def extract_submission_infos(self, idx: Hashable, study_key: str, study_data: pd.Series) -> Tuple:
        pass

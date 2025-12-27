from typing import Dict, List, Any, Hashable, Tuple
import logging
from datetime import datetime
import pandas as pd
import hashlib

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

    @staticmethod
    def deep_get(data, path: str):
        """Navigate nested dict using dot separated path, or flat Series/dict."""
        if isinstance(data, pd.Series):
            return data.get(path)

        # dict navigation
        keys = path.split(".")
        value = data
        for key in keys:
            if isinstance(value, dict):
                value = value.get(key)
            else:
                return None
        return value

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

        all_study_arm_groups = []
        all_study_arm_group_interventions = []

        all_interventions = []
        all_interventions_other_names = []
        all_study_interventions = []

        df_studies = pd.read_parquet(data_loc)
        df_studies = pd.json_normalize(df_studies["studies"])

        for study in df_studies:

            nct_id = self.deep_get(study, "protocolSection.identificationModule.nctId")
            if not nct_id:
                self.log.warning("Study missing NCT ID, skipping")  # log page and index
                continue

            study_key = self.generate_key(nct_id)

            # study
            study_record = self.extract_study_fields(study_key, study)
            all_studies.append(study_record)

            # sponsors
            sponsors, study_sponsors = self.extract_sponsors(study_key, study)
            all_sponsors.extend(sponsors)
            all_study_sponsors.extend(study_sponsors)

            # conditions and keywords
            conditions, study_conditions = self.extract_conditions(study_key, study)
            all_conditions.extend(conditions)
            all_study_conditions.extend(study_conditions)

            keywords, study_keywords = self.extract_keywords(study_key, study)
            all_keywords.extend(keywords)
            all_study_keywords.extend(study_keywords)

            # groups and interventions
            arm_groups, arm_group_interventions = self.extract_arm_groups(
                study_key, study
            )
            all_study_arm_groups.extend(arm_groups)
            all_study_arm_group_interventions.extend(arm_group_interventions)

            interventions, intervention_other_names, study_interventions = (
                self.extract_interventions(study_key, study)
            )
            all_interventions.extend(interventions)
            all_interventions_other_names.extend(intervention_other_names)
            all_study_interventions.extend(study_interventions)

        # build dataframes from lists
        df_studies = pd.DataFrame(all_studies)

        df_sponsors = pd.DataFrame(all_sponsors)
        df_study_sponsors = pd.DataFrame(all_study_sponsors)

        df_conditions = pd.DataFrame(all_conditions)
        df_study_conditions = pd.DataFrame(all_study_conditions)

        df_keywords = pd.DataFrame(all_keywords)
        df_study_keywords = pd.DataFrame(all_study_keywords)

        df_study_arm_groups = pd.DataFrame(all_study_arm_groups)
        df_study_arm_group_interventions = pd.DataFrame(
            all_study_arm_group_interventions
        )

        df_interventions = pd.DataFrame(all_interventions)
        df_intervention_other_names = pd.DataFrame(all_interventions_other_names)
        df_study_interventions = pd.DataFrame(all_study_interventions)

        # dedupe
        df_sponsors = df_sponsors.drop_duplicates(subset=["sponsor_key"])
        df_conditions = df_conditions.drop_duplicates(subset=["condition_key"])
        df_keywords = df_keywords.drop_duplicates(subset=["keyword_key"])
        df_interventions = df_interventions.drop_duplicates(subset=["intervention_key"])
        df_intervention_other_names = df_intervention_other_names.drop_duplicates(
            subset=["intervention_key", "other_name"]
        )

        # load
        return df_studies, df_sponsors, df_study_sponsors

    def extract_study_fields(self, study_key: str, study_df: dict) -> Dict:
        study_record = dict()

        study_record["study_key"] = study_key
        for entity_key in SINGLE_FIELDS:
            index_field = SINGLE_FIELDS.get(entity_key)

            study_record[entity_key] = self.deep_get(study_df, index_field)

        return study_record

    def extract_sponsors(self, study_key: str, study_data: Dict):
        """
        Extract sponsors from a single study.
        Args:
            study_key: The generated key for this study
            study_data: The nested dict for one study (not a DataFrame)
        Returns:
            Tuple of (sponsors_list, study_sponsors_list)
        """

        sponsors = []
        study_sponsors = []

        # Extract lead sponsor
        lead_sponsor_index = "protocolSection.sponsorCollaboratorsModule.leadSponsor"
        lead_sponsor = self.deep_get(study_data, lead_sponsor_index)

        if lead_sponsor:
            sponsor_key = self.generate_key(
                lead_sponsor.get("name"), lead_sponsor.get("class")
            )

            sponsors.append(
                {
                    "sponsor_key": sponsor_key,
                    "name": lead_sponsor.get("name"),
                    "sponsor_class": lead_sponsor.get("class"),
                }
            )

            study_sponsors.append(
                {"study_key": study_key, "sponsor_key": sponsor_key, "is_lead": True}
            )

        # Extract collaborators
        collaborators_path = "protocolSection.sponsorCollaboratorsModule.collaborators"
        collaborators_list = self.deep_get(study_data, collaborators_path) or []

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

    def extract_conditions(self, study_key: str, study_data: Dict) -> Tuple:
        conditions = []
        study_conditions = []

        conditions_index = NESTED_FIELDS["conditions"]["index_field"]
        conditions_list = self.deep_get(study_data, conditions_index)

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

        return conditions, study_conditions

    def extract_keywords(self, study_key: str, study_data: Dict) -> Tuple:
        keywords = []
        study_keywords = []

        keywords_index = NESTED_FIELDS["keywords"]["index_field"]
        keywords_list = self.deep_get(study_data, keywords_index)

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

    def extract_arm_groups(self, study_key: str, study_data: Dict) -> Tuple[List, List]:
        study_arms = []
        study_arms_interventions = []

        study_arms_index = NESTED_FIELDS["arm_groups"]["index_field"]
        study_arms_list = self.deep_get(study_data, study_arms_index) or []

        for study_arm in study_arms_list:
            study_arm_key = self.generate_key(study_key, study_arm.get("label"))

            study_arms.append(
                {
                    "study_arm_key": study_arm_key,
                    "study_key": study_key,
                    "label": study_arm.get("label"),
                    "description": study_arm.get("description"),
                    "type": study_arm.get("type"),
                }
            )

            arm_interventions = study_arm.get("interventionNames") or []

            for intervention in arm_interventions:
                study_arms_interventions.append(
                    {
                        "study_key": study_key,
                        "study_arm_key": study_arm_key,
                        "intervention_name": intervention,
                    }
                )

        return study_arms, study_arms_interventions

    def extract_interventions(self, study_key: str, study_data: Dict) -> Tuple:
        interventions = []
        study_interventions = []
        intervention_other_names = []

        interventions_index = NESTED_FIELDS["interventions"]["index_field"]
        interventions_list = self.deep_get(study_data, interventions_index) or []

        for intervention in interventions_list:
            intervention_key = self.generate_key(study_key, intervention.get("name"))

            interventions.append(
                {
                    "intervention_key": intervention_key,
                    "name": intervention.get("name"),
                    "description": intervention.get("description"),
                    "type": intervention.get("type"),
                }
            )

            study_interventions.append(
                {
                    "study_key": study_key,
                    "intervention_key": intervention_key,
                }
            )

            other_names = intervention.get("otherNames") or []
            for other_name in other_names:
                intervention_other_names.append(
                    {
                        "intervention_key": intervention_key,
                        "other_name": other_name,
                    }
                )

        return interventions, intervention_other_names, study_interventions

    def extract_locations(self, study_key: str, study_data: Dict) -> Tuple:
        pass

    def extract_officials(self, study_key: str, study_data: Dict) -> Tuple:
        pass

    def extract_outcomes(self, study_key: str, study_data: Dict) -> Tuple:
        pass

    def extract_see_also(self, study_key: str, study_data: Dict) -> Tuple:
        pass

    def extract_study_phases(self, study_key: str, study_data: Dict) -> Tuple:
        pass

    def extract_age_group(self, study_key: str, study_data: Dict) -> Tuple:
        pass

    def extract_ipd_info_types(self, study_key: str, study_data: Dict) -> Tuple:
        pass

    def extract_id_infos(self, study_key: str, study_data: Dict) -> Tuple:
        pass

    def extract_nct_id_aliases(self, study_key: str, study_data: Dict) -> Tuple:
        pass

    def extract_condition_mesh(self, study_key: str, study_data: Dict) -> Tuple:
        pass

    def extract_intervention_mesh(self, study_key: str, study_data: Dict) -> Tuple:
        pass

    def extract_large_documents(self, study_key: str, study_data: Dict) -> Tuple:
        pass

    def extract_unposted_events(self, study_key: str, study_data: Dict) -> Tuple:
        pass

    def extract_violation_events(self, study_key: str, study_data: Dict) -> Tuple:
        pass

    def extract_removed_countries(self, study_key: str, study_data: Dict) -> Tuple:
        pass

    def extract_submission_infos(self, study_key: str, study_data: Dict) -> Tuple:
        pass

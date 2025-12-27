from typing import Dict, List, Tuple, Hashable
import logging
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
            arm_groups, arm_group_interventions = self.extract_arm_groups(
                idx, study_key, study
            )
            all_study_arm_groups.extend(arm_groups)
            all_study_arm_group_interventions.extend(arm_group_interventions)

            interventions, intervention_other_names, study_interventions = self.extract_interventions(idx, study_key, study)

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
            self.log.warning(f"No lead sponsor found for {idx}")


        # Extract collaborators
        collaborators_index = NESTED_FIELDS["collaborators"]["index_field"]
        collaborators_list = study_data.get(collaborators_index)

        if collaborators_list is not None and len(collaborators_list) > 0:
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


        if conditions_list is not None and len(conditions_list) > 0:
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


        self.log.warning(f"No conditions found for {idx}")
        return conditions, study_conditions


    def extract_keywords(self, idx: Hashable, study_key: str, study_data: pd.Series) -> Tuple:
        keywords = []
        study_keywords = []

        keywords_index = NESTED_FIELDS["keywords"]["index_field"]
        keywords_list = study_data.get(keywords_index)

        if keywords_list is not None and len(keywords_list) > 0:

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



    def extract_arm_groups(self, idx: Hashable, study_key: str, study_data: pd.Series) -> Tuple[List, List] | None:
        study_arms = []
        study_arms_interventions = []

        study_arms_index = NESTED_FIELDS["arm_groups"]["index_field"]
        study_arms_list = study_data.get(study_arms_index)

        if study_arms_list is not None and len(study_arms_list) > 0:

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

        self.log.warning(f"No arms found for {idx}")

        return study_arms, study_arms_interventions

    def extract_interventions(self, idx: Hashable, study_key: str, study_data: pd.Series) -> Tuple | None:
        interventions = []
        study_interventions = []
        intervention_other_names = []

        interventions_index = NESTED_FIELDS["interventions"]["index_field"]
        interventions_list = study_data.get(interventions_index)

        if interventions_list is not None and len(interventions_list) > 0:
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

                other_names = intervention.get("otherNames")
                if other_names is not None and len(other_names) > 0:
                    for other_name in other_names:
                        intervention_other_names.append(
                            {
                                "intervention_key": intervention_key,
                                "other_name": other_name,
                            }
                        )

        self.log.warning(f"No interventions found for {idx}")
        return interventions, intervention_other_names, study_interventions



    def extract_locations(self, idx: Hashable, study_key: str, study_data: pd.Series) -> Tuple:
        pass

    def extract_officials(self, idx: Hashable, study_key: str, study_data: pd.Series) -> Tuple:
        pass

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

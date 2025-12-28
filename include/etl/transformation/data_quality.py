from typing import Set

class DataQualityHandler:
    def __init__(self):
        pass

    @staticmethod
    def resolve_location_status(location_statuses: Set) -> str:
        """
        Resolve conflicting location statuses.

        Priority:
        1. Single status -> use it
        2. {RECRUITING, NOT_YET_RECRUITING} -> RECRUITING (progression)
        3. RECRUITING + final status -> final status (study ended)
        4. RECRUITING + other -> RECRUITING_STATUS_UNCLEAR
        """

        if not location_statuses:
            return "UNKNOWN"

        if len(location_statuses) == 1:
            return list(location_statuses)[0]

        # progression case
        if location_statuses == {"RECRUITING", "NOT_YET_RECRUITING"}:
            return "RECRUITING"

        final_statuses = ["COMPLETED", "TERMINATED", "WITHDRAWN"]

        if "RECRUITING" in location_statuses:
            for final_status in final_statuses:
                if final_status in location_statuses:
                    return final_status  # study ended, can't recruit

            # RECRUITING plus other ambiguous statuses
            return "RECRUITING_STATUS_UNCLEAR"

        # Multiple non-recruiting statuses - anyone works. makes no difference
        return list(location_statuses)[0]

    

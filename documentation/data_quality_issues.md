## LOCATION STATUS DUPLICATES

Location-level statuses can be inconsistent/outdated. Use study-level overall_status as the authoritative source:

1. Study COMPLETED/TERMINATED -> inherit study status (can't recruit)
2. Study NOT_YET_RECRUITING -> inherit study status (hasn't started)
3. Study RECRUITING + conflicting locations AND one is RECRUITING -> UNCLEAR
4. Study RECRUITING + no location says RECRUITING -> use location status





## PARTICIPANT FLOW DUPLICATES

### Issue
Some studies contain duplicate period entries with the same title  but different participant counts. 

This appears to be a data entry error or represents updates/corrections to enrollment numbers.

**Example: `NCT01614769`
  Period "7-Day Washout" appears twice for group FG000:
  - First entry: 5 participants started
  - Second entry: 3 participants started
  
**Resolution**:Aggregate duplicate (study, period, event, group) combinations  by SUMMING num_subjects. 

This assumes multiple entries represent cumulative enrollment or separate cohorts within the same period.

Rationale:
- Preserves all participant count data
- Avoids arbitrary choice of "which entry is correct" SINCE THEY AREN'T DATED
- Provides total enrollment across all period entries


**Limitation:** If entries represent corrections (not additions), totals may  be inflated. 


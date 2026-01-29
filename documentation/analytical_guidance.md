# Data Quality Policies


## Overview
This data warehouse contains clinical trial data extracted from ClinicalTrials.gov. While the extraction and transformation process is robust,
the source data has known quality issues. This guide helps analysts understand data limitations and write resilient queries.

A 2019 study (Zarin et al., New England Journal of Medicine) documented systematic quality issues in ClinicalTrials.gov:

12% of trials: Missing investigator information
17% of "names": Junk data instead of real person names
13% of investigator names: Variants that cannot be resolved programmatically
Multiple entries: Same information listed multiple times with conflicts e.g MeSH terms, intervention names duplicated in other intervention names


### Normalized Structure with Bubbled Keys 

The transformation layer produces a cleaned staging schema that balances separate normalized tables and parent keys propagated to children for to avoid 
multi hop joins during dimensional modelling


### Surrogate Key Propagation

All child tables include direct foreign keys to ancestor tables:
```sql
outcome_measurements
- measurement_key (PK)
- study_key (FK)        -- Grandparent
- outcome_key (FK)      -- Parent
- group_key (FK)        -- Direct parent
```



### All boolean fields

Fields are  asserted only when true; absence does not imply false.


## Field-Specific Data Quality Guidance

### Study Status

`studies.overall_status` 


#### ISSUE: Overall status is DERIVED from location statuses and is not authoritative.

**ACTUAL:** From location-level data (most reliable)
**OVERALL:** Inherited from study overall_status (fallback)
**INFERRED:** Derived from resolution logic (flag for review)

**Policy:** Always filter or flag by status_type for transparency.

---

## Timestamps

**Policy:** Use `completion_date` for time series of completed trials. Use `start_date` for trial initiation trends.

**Reason:** Some dates are `ACTUAL`, some are `ESTIMATED`. Check `date_type` field if precision matters.

---

## Eligibility Criteria

`studies.eligibility_criteria` 

Free text field. Structured eligibility parsing (age, biomarkers, lab values) is a separate extraction layer implemented by the patient matching API.

**Policy:** Treat as unstructured text. Do not query with exact string matching.

---


## Location Data

#### ISSUE: Facility names are inconsistent ("Johns Hopkins" vs "Johns Hopkins Hospital" vs "JHU Medical Center")

**Policy:** Filter by `country` for geographic analysis. Do not rely on `facility` for aggregation.

---


## Sponsor/Collaborator Data

#### ISSUE: sponsor names have minor variations e.g "Pfizer" vs "Pfizer Inc" vs "Pfizer Inc." may appear as separate entities.

---


### Participant Flow Data

`flow_period_events`

#### ISSUE: Some studies contain duplicate period entries (same title, different participant counts).

**Resolution**: I assume these are different flows and the pipeline aggregates by SUMMING `num_subjects` for duplicate periods.

**Example**:
```
Source data:
- Period "7-Day Washout", Group FG000: 5 participants
- Period "7-Day Washout", Group FG000: 3 participants (duplicate)

```

#### ISSUE: Missing Data vs Zero Participants


num_subjects = NULL: Data not reported (missing)
num_subjects = 0: Zero participants (reported)


**POLICY: Always filter NULL values explicitly in calculations.**

----


###  Intervention Data

#### ISSUE:  Intervention names are sponsor-entered free text without validation. The same drug appears as "5-FU", "5-fluorouracil", "Fluorouracil", "fluorouracil 500mg IV". MeSH terms are standardized by NLM.

**Policy:** Use MeSH terms (`interventions_mesh`) for analytical queries. Use `intervention_names` for patient-facing search and display.

**Exception:** When MeSH term is missing, fall back to `intervention_names` 

---

### Condition Data

#### ISSUE:  "Breast cancer", "Breast Carcinoma", "breast neoplasm", "Cancer of Breast" are all entered differently by sponsors.

**Policy:** Use MeSH terms (`conditions_mesh`) for analytical queries. Use raw `conditions` for patient-facing search.


---

## Arm Groups

#### ISSUE:  Arm labels and descriptions are study-specific free text. "Treatment Arm A" in one study has no relationship to "Treatment Arm A" in another.

**Policy:** Do not attempt to standardize or aggregate across studies.

---



## Outcome Measures vs Outcomes

**Policy:**
- `outcomes` = protocol-defined endpoints (what they planned to measure)
- `outcome_measures` = actual results (what they found)

**Reason:** Different modules in source data. Only completed studies with posted results have `outcome_measures`.

---



## Results Data (Participant Flow, Adverse Events, Outcome Measures)

#### ISSUE:  Results reporting is often delayed or missing. FDAAA 801 violations in `violations` table indicate non-compliance.


**Policy:** Only available for studies with `results_posted = true`. 


---




## Sparse/Incomplete Fields

- Browse Leaves/Branches: Missing in 100% of sampled studies
- Detailed Subgroup Analyses: Only primary outcome results extracted

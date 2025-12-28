DATA EXTRACTION NOTES - JSON Normalization 
==========================================

When using pd.json_normalize() on the ClinicalTrials.gov API response,
nested structures are flattened differently based on their type:

1. SCALAR VALUES (strings, numbers, booleans):
   - Preserved as single columns with dot-notation paths
   - Example: 'protocolSection.identificationModule.nctId' -> single column
   - Must be extracted directly: study_data.get('protocolSection.identificationModule.nctId')

2. NESTED DICTIONARIES (objects):
   - Flattened into separate columns for each field
   - Example: leadSponsor: {"name": "X", "class": "Y"}
     Becomes TWO columns:
     - 'protocolSection.sponsorCollaboratorsModule.leadSponsor.name'
     - 'protocolSection.sponsorCollaboratorsModule.leadSponsor.class'
   - CANNOT extract as: study_data.get('...leadSponsor') -> Returns None
   - MUST extract fields individually:
     study_data.get('...leadSponsor.name')
     study_data.get('...leadSponsor.class')

3. ARRAYS OF DICTIONARIES:
   - Creates multiple rows OR remains as list depending on normalize depth
   - Example: collaborators: [{"name": "A", "class": "B"}, ...]
   - May need special handling depending on max_level parameter

IMPLICATION FOR EXTRACTION FUNCTIONS:
- extract_study_fields(): Works with scalar values -> direct path extraction
- extract_sponsors(): Must extract individual fields from flattened dicts
- extract_collaborators(): May need to handle arrays differently


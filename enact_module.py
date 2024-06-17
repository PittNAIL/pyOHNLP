import pandas as pd

#cols = encounter_num, concept_cd, provider_id, start_date, patient_num, modifier_cd, instance_num,
#valtype_cd, tval_char, nval_num, valueflag_cd, quantity_num, units_cd, end_date, location_cd,
#observation_blob, confidence_num, update_date, download_date, import_date, sourcesystem_cd,
#upload_id

#meta_data = provider_id, start_date, patient_num, project_name (MIGHT NOT BE APPLICABLE HERE..

def enact_transform(dtc: pd.DataFrame):
    print(dtc)
    data_keys = dtc.keys()
    if "project_name" not in data_keys:
        project_name = input("Missing project name! What is the project for this run?")
